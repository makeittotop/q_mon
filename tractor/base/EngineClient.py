"""
This module provides an interface for communicating with the Tractor engine using the URL API.
"""

import os, json, urllib2, ast, getpass, re, platform

import TrHttpRPC
import tractor.base.rpg
import rpg.Formats as Formats
import rpg.progutil as progutil
import rpg.osutil as osutil

DEFAULT_ENGINE = "tractor-engine:80"
DEFAULT_ENGINE_PORT = 80
DEFAULT_USER = "root"
DEFAULT_PASSWORD = None

def hostnamePortForEngine(engineName):
    """Return (hostname, port) for the given engine string.  Defaults to port 80 if none specified."""

    parts = engineName.split(":")
    if len(parts) == 1:
        return (engineName, DEFAULT_ENGINE_PORT)
    else:
        if not parts[1].isdigit():
            raise EngineError("'%s' must be a numeric value for port." % parts[1])
        return (parts[0], int(parts[1]))

def rendermanPrefsDir():
    """Return path to tractor preferences directory in standard renderman location."""
    system = platform.system()
    if system == "Darwin":
        homeDir = os.environ.get("HOME", "/tmp")
        prefsDir = os.path.join(homeDir, "Library", "Preferences", "Pixar", "Tractor")
    elif system == "Windows":
        appDir = os.environ.get("APPDATA", "/tmp")
        prefsDir = os.path.join(appDir, "Pixar", "Tractor")
    else: # assume system is Linux
        homeDir = os.environ.get("HOME", "/tmp")
        prefsDir = os.path.join(homeDir, ".pixarPrefs", "Tractor")
    return prefsDir

class EngineClientError(Exception):
    """Base class for EngineClient exceptions."""
    pass

class InvalidParamError(EngineClientError):
    """Raised when an attempt has been made to modify an invalid connection parameter."""
    pass

class OpenConnError(EngineClientError):
    """Raised when there is a problem opening a connection with the engine."""
    pass

class LoginError(EngineClientError):
    """Raised when there is a problem loggin in to the engine."""
    pass

class TransactionError(EngineClientError):
    """Raised when the engine returns a non-zero return code."""
    pass

class DBExecError(EngineClientError):
    """Raised when there is a postgres error executing arbitrary SQL using the
    EngineClient.dbexec() function."""
    pass

class DictObj(object):
    def __init__(self, **kwargs):
        for key,val in kwargs.items():
            setattr(self, key, val)

class EngineClient(object):
    """This class is used to manage connections with the engine."""
    
    SESSION_FILENAME_BASE = "session"
    QUEUE = "queue"
    MONITOR = "monitor"
    CONTROL = "ctrl"
    SPOOL = "spool"
    DB = "db"

    LIMITS_CONFIG_FILENAME = "limits.config"
    SPOOL_VERSION = "2.0"
    
    def __init__(self, hostname=None, port=None, user=None, password=None, debug=False):
        # connection parameters
        fallbackHostname, fallbackPort = hostnamePortForEngine(os.environ.get("TRACTOR_ENGINE", DEFAULT_ENGINE))
        self.hostname = hostname or fallbackHostname
        self.port = port or fallbackPort
        self.user = user or os.environ.get("USER", DEFAULT_USER)
        self.password = password or os.environ.get("TRACTOR_PASSWORD", DEFAULT_PASSWORD)
        self.debug = debug or os.environ.get("TRACTOR_DEBUG")

        # create descriptive headers for readability purposes in server logs
        appName = "EngineClient"
        appVersion = "1.0"
        appDate = "app date"
        self.lmthdr = {
            'User-Agent': "Pixar-%s/%s (%s)" % (appName, appVersion, appDate),
            'X-Tractor-Blade': "0"
            }

        # gets set to True for open() to explicitly open new connection
        self.newSession = False
        # session id with engine
        self.tsid = None
        # TrHttpRPC connection with engine
        self.conn = None

    def xheaders(self):
        # dynamically generate xheaders so that it can adapt to a reconfiguration of the hostname or port
        return {
            'Host': "%s:%s" % (self.hostname, self.port),
            'Cookie': "TractorUser=%s" % self.user
            }

    def setParam(self, **kw):
        """Set one or more connection parameters: hostname, port, user, password, and debug."""
        # if engine is specified, replace the class engine client object
        for key, value in kw.iteritems():
            if key not in ("hostname", "port", "user", "password", "debug", "newSession"):
                raise InvalidParamError("%s is not a valid parameter." % str(key))
            setattr(self, key, value)

    def sessionFilename(self):
        return os.path.join(self.prefsDir(), "%s.%s" % (osutil.getlocalhost(), self.SESSION_FILENAME_BASE))

    def isOpen(self):
        """Return True if a connection is considered to have been established."""
        # a known session id is considered to represent an established connection
        return self.tsid is not None

    def dprint(self, msg):
        """Display message when running in debug mode."""
        if self.debug:
            progutil.log("[%s:%s] %s" % (self.hostname, self.port, msg))

    def dprintUrl(self, url):
        """Display url when running in debug mode."""
        if self.debug:
            progutil.log("[%s:%s] http://%s:%s/Tractor/%s" % (self.hostname, self.port, self.hostname, self.port, url))

    def prefsDir(self):
        """Return the path to the preferences directory for a client with this engine."""
        engineID = "%s@%s" % (self.hostname, self.port)
        return os.path.join(rendermanPrefsDir(), "sites", engineID)

    def createPrefsDir(self):
        """Create the preferences directory for a client with this engine."""
        d = self.prefsDir()
        if os.path.exists(d):
            return
        # IO/OSErrors will be passed on to caller
        os.makedirs(d, 0700)

    def reuseSession(self):
        """Return True if prior session can be used to communicate with engine."""
        if not os.path.exists(self.sessionFilename()):
            return False
        try:
            f = open(self.sessionFilename())
            sessionInfo = json.load(f)
            f.close()
        except (IOError, OSError, ValueError), err:
            progutil.logWarning("problem reading session file: %s" % str(err))
            return False
        self.tsid = sessionInfo.get("tsid")

        # test session id
        try:
            self.ping()
        except EngineClientError, err:
            self.dprint(str(err))
            return False
        else:
            # session id must be good
            return True

    def open(self, newSession=False):
        """Establish connection with engine."""
        self.conn = TrHttpRPC.TrHttpRPC(self.hostname, port=self.port, apphdrs=self.lmthdr, timeout=3600)

        if not self.password:
            if not self.newSession and not newSession and self.reuseSession():
                self.dprint("reuse engine connection")
                return

            passwordRequired = self.conn.PasswordRequired()
            if passwordRequired and not self.password:
                self.password = getpass.getpass("Enter password for %s@%s:%d: " % (self.user, self.hostname, self.port))
            
        self.dprint("open engine connection")
        try:
            response = self.conn.Login(self.user, self.password)
        except TrHttpRPC.TrHttpError, err:
            self.tsid = None
            raise OpenConnError(str(err))

        self.tsid = response['tsid']
        if self.tsid == None:
            msg = "unable to log in user: %s" % self.user
            self.dprint(msg)
            raise LoginError(msg)
        # save out login data
        try:
            self.createPrefsDir()
            with os.fdopen(os.open(self.sessionFilename(), os.O_WRONLY | os.O_CREAT, 0600), "w") as f:
                f.write('{"tsid": "%s"}\n' % self.tsid);
        except (IOError, OSError), err:
            progutil.logWarning("problem writing session file: %s" % str(err))

    def constructURL(self, queryType, keyValuePairs):
        """Build a URL."""
        parts = []
        for key, value in keyValuePairs.iteritems():
            if type(value) is list:
                # this will automatically change lists into comma-separated values. e.g. [1,3,5] => '1,3,5'
                value = ",".join([str(v) for v in value])
            parts.append("%s=%s" % (key, urllib2.quote(str(value))))
        if self.tsid:
            parts.append("tsid=%s" % self.tsid)
        return queryType + "?" + "&".join(parts)
        
    def _shortenTraceback(self, msg):
        """Extract the important part of a traceback."""
        # extract the error message displayed on the line after the line containing "raise" 
        matches = re.findall("\n\s*raise .*\n(\w+\:.*)\n", msg)
        if matches:
            # choose the last exception displayed (with [-1]) and the first element [0] has the full message
            return matches[-1]
        # sometimes there is no raise line, but there is a CONTEXT line afterwards
        matches = re.findall("\n(\w+\:.*)\n\nCONTEXT\:", msg)
        if matches:
            # choose the last exception displayed (with [-1]) and the first element [0] has the full message
            return matches[-1]
        return msg
        
    def _transaction(self, urltype, attrs, payload=None, translation="JSON", headers={}, skipLogin=False):
        """Send URL to engine, parse and return engine's response."""
        # support lazy opening of connection
        if skipLogin:
            # login is skipped for spooling
            if not self.conn:
                self.conn = TrHttpRPC.TrHttpRPC(self.hostname, port=self.port, apphdrs=self.lmthdr, timeout=3600)
        else:
            if not self.isOpen():
                self.open()
        url = self.constructURL(urltype, attrs)
        self.dprintUrl(url)
        headers = headers.copy() # copy so we don't modify dictionary with update()
        headers.update(self.xheaders())
        rcode, data = self.conn.Transaction(url, payload, translation, headers)
        if rcode:
            try:
                datadict = ast.literal_eval(str(data))
                err = datadict.get("msg", "unknown message")
                if self.debug:
                    msg = "[%s:%d] error %s: %s" % (self.hostname, self.port, datadict.get("rc", "unknown rc"), err)
                else:
                    msg = self._shortenTraceback(err)
            except (SyntaxError, ValueError), err:
                msg = str(data)
            raise TransactionError(msg)
        return data

    def dbexec(self, sql):
        """Execute an arbitrary SQL statement on the postgres server, using the engine as a proxy.
        The result will be a dictionary, with one entry being a JSON encoded list of the
        result rows."""
        self.dprint("sql = %s" % sql)
        result = self._transaction(self.DB, {"q": sql})
        # an error could be reported through either:
        #  rc: for psql client errors
        #  rows: for tractorselect traceback errors, such as for syntax errors in search clause 
        rc = result.get("rc", 1)
        rows = result.get("rows")
        isError = type(rows) != list
        self.dprint("rc=%d, isError=%s" % (rc, isError))
        if rc:
            err = result.get("msg") or "postgres server did not specify an error message for dbexec(%s)" % sql
        elif isError:
            err = rows # rows is a string here
        else:
            err = None
            
        if err:
            if self.debug:
                # return full stack trace from server
                err = "error message from postgres server:\n" + \
                      "---------- begin error ----------\n" + err + \
                      "----------- end error -----------"
            else:
                # just set the message to a exception if one existed
                #err = self._shortenTraceback(err)
                err = err.strip()
                errLines = err.split("\n")
                err = errLines[-1]
            raise DBExecError(err)
        
        return rows
        
    def select(self, tableName, where, columns=[], sortby=[], limit=0, archive=False, aliases=None):
        """Select items from the specified table, using the given natural language where clause."""
        sql = "tractorselect('%s', '%s', '%s', '%s', %s, '%s', '%s')" % \
            (tableName,
             where.replace("'", "''"),
             ",".join(columns),
             ",".join(sortby or []),
             "NULL" if limit is None else str(limit),
             't' if archive else 'f',
             str(aliases).replace("'", "''"))
        rows = self.dbexec(sql)
        return rows

        attrs = {"q": "select", "table": tableName, "where": where, "columns": ",".join(columns), 
                 "orderby": ",".join(orderby), "limit": str(limit)}
        result = self._transaction(self.MONITOR, attrs)
        # result is a dictionary with a "rows" entry that is a list of key/value pairs
        return result

    def _setAttributeJob(self, jid, attribute, value):
        """Set a job's attribute to the specified value."""
        attrs = {"q": "jattr", "jid": jid, "set_"+attribute: value}
        self._transaction(self.QUEUE, attrs)

    def _setAttributeCommand(self, jid, cid, attribute, value):
        """Set a command's attribute to the specified value."""
        attrs = {"q": "cattr", "jid": jid, "cid": cid, "set_"+attribute: value}
        self._transaction(self.QUEUE, attrs)

    def _setAttributeBlade(self, bladeName, ipaddr, attribute, value):
        """Set a blade's attribute to the specified value."""
        bladeId = "%s/%s" % (bladeName, ipaddr)
        attrs = {"q": "battribute", "b": bladeId, attribute: value}
        self._transaction(self.CONTROL, attrs)

    def setJobPriority(self, jid, priority):
        """Set a job's priority."""
        self._setAttributeJob(jid, "priority", priority)

    def setJobCrews(self, jid, crews):
        """Set a job's crew list."""
        self._setAttributeJob(jid, "crews", ",".join(crews))

    def setJobAttribute(self, jid, key, value):
        """Set a job's attribute to the specified value."""
        if type(value) == list:
            value = ",".join([str(v) for v in value])
        self._setAttributeJob(jid, key, value)

    def pauseJob(self, jid):
        """Pause a job."""
        self._setAttributeJob(jid, "pause", 1)

    def unpauseJob(self, jid):
        """Unpause a job."""
        self._setAttributeJob(jid, "pause", 0)

    def interruptJob(self, jid):
        """Interrupt a job."""
        attrs = {"q": "jinterrupt", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def restartJob(self, jid):
        """Restart a job."""
        attrs = {"q": "jrestart", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def retryAllActiveInJob(self, jid):
        """Retry all active tasks of a job."""
        attrs = {"q": "jretry", "tsubset": "active", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def retryAllErrorsInJob(self, jid):
        """Retry all errored tasks of a job."""
        attrs = {"q": "jretry", "tsubset": "error", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def skipAllErrorsInJob(self, jid):
        """Skip all errored tasks of a job."""
        attrs = {"q": "tskip", "tsubset": "error", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def delayJob(self, jid, delayTime):
        """Set delay time of a job."""
        self.setJobAttribute(jid, "afterTime", str(delayTime))

    def undelayJob(self, jid):
        """Clear delay time of a job."""
        self.setJobAttribute(jid, "afterTime", "0")

    def deleteJob(self, jid):
        """Delete a job."""
        attrs = {"q": "jretire", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def undeleteJob(self, jid):
        """Un-delete a job."""
        attrs = {"q": "jrestore", "jid": jid}
        self._transaction(self.QUEUE, attrs)

    def retryTask(self, jid, tid):
        """Retry a task."""
        attrs = {"q": "tretry", "jid": jid, "tid": tid}
        self._transaction(self.QUEUE, attrs)

    def resumeTask(self, jid, tid):
        """Resume a task."""
        attrs = {"q": "tretry", "recover": 1, "jid": jid, "tid": tid}
        self._transaction(self.QUEUE, attrs)

    def skipTask(self, jid, tid):
        """Skip a task."""
        attrs = {"q": "tskip", "jid": jid, "tid": tid}
        self._transaction(self.QUEUE, attrs)

    def setCommandAttribute(self, jid, cid, key, value):
        """Set a command's attribute to the specified value."""
        if type(value) == list:
            value = ",".join([str(v) for v in value])
        self._setAttributeCommand(jid, cid, key, value)

    def getTaskCommands(self, jid, tid):
        """Return the command details for a task."""
        attrs = {"q": "taskdetails", "jid": jid, "tid": tid}
        result = self._transaction(self.MONITOR, attrs)
        if not result.has_key("cmds"):
            return
        lines = []
        cmds = result["cmds"]

        formats = [
            Formats.IntegerFormat("cid", width=4),
            Formats.StringFormat("state", width=8),
            Formats.StringFormat("service", width=12),
            Formats.StringFormat("tags", width=12),
            Formats.StringFormat("type", width=4),
            Formats.TimeFormat("t0", header="start", width=11),
            Formats.TimeFormat("t1", header="stop", width=11),
            Formats.ListFormat("argv", header="command")
            ]
        cmdFormatter = Formats.Formatter(formats)

        if len(cmds) > 0:
            lines.append(cmdFormatter.header())
        o = object()
        for cmd in cmds:
            cmdObj = DictObj(**cmd)
            line = cmdFormatter.format(cmdObj)
            lines.append(line)
        
        return "\n".join(lines)
        
    def getTaskLog(self, jid, tid, owner=None):
        """Return the command logs for a task."""
        attrs = {"q": "tasklogs", "jid": jid, "tid": tid}
        if owner:
            attrs["owner"] = owner
        logInfo = self._transaction(self.MONITOR, attrs)
        logLines = []
        if not logInfo.has_key("LoggingRedirect"):
            return ""

        logURIs = logInfo["LoggingRedirect"]
        for logURI in logURIs:
            fullURI = "http://%s:%s%s" % (self.hostname, self.port, logURI)
            # fetch the log
            try:
                f = urllib2.urlopen(fullURI)
            except Exception, err:
                logResult = "Exception received in EngineClient while fetching log: %s" % str(err)
            else:
                logResult = f.read()
                f.close()
            # append it to result, since there may be mutiple URIs
            logLines.append(logResult)

        return "".join(logLines)

    def fetchJobsAsJSON(self, filterName=None):
        """Return the job description in JSON format."""
        attrs = {"q": "jobs"}
        if filterName:
            attrs["filter"] = filterName + ".joblist"
        jobInfo = self._transaction(self.MONITOR, attrs)
        return jobInfo

    def fetchBladesAsJSON(self, filterName=None):
        """Return the status of all blades in JSON format."""
        attrs = {"q": "blades"}
        if filterName:
            attrs["filter"] = filterName + ".bladelist"
        bladeInfo = self._transaction(self.MONITOR, attrs)
        return bladeInfo

    def nimbyBlade(self, bladeName, ipaddr):
        """Nimby a bade."""
        self._setAttributeBlade(bladeName, ipaddr, "nimby", 1)

    def unnimbyBlade(self, bladeName, ipaddr):
        """Unnimby a bade."""
        self._setAttributeBlade(bladeName, ipaddr, "nimby", 0)

    def traceBlade(self, bladeName, ipaddr):
        """Return the tracer output for a blade."""
        bladeId = "%s/%s" % (bladeName, ipaddr)
        attrs = {"q": "tracer", "t": bladeId, "fmt": "plain"}
        trace = self._transaction(self.CONTROL, attrs, translation=None) or ""
        return trace

    def reloadLimitsConfig(self):
        """Cause the engine the reload the limits.config file."""
        attrs = {"q": "reconfigure", "file": self.LIMITS_CONFIG_FILENAME}
        self._transaction(self.CONTROL, attrs)
        
    def queueStats(self):
        """Return the engine's current queue statistics."""
        attrs = {"q": "status", "qlen": "1", "enumq": "1"}
        return self._transaction(self.CONTROL, attrs)

    def ping(self):
        """Perform simple communication with engine to verify the session is valid."""
        attrs = {"q": "status"}
        self._transaction(self.CONTROL, attrs)

    def spool(self, jobData, hostname=None, filename=None, owner=None, format=None, skipLogin=False, block=False):
        """Spool the given job data."""
        hostname = hostname or osutil.getlocalhost()
        owner = owner or self.user or getpass.getuser()
        cwd = os.path.abspath(os.getcwd()).replace('\\', '/')
        filename = filename or "no filename specified"
        attrs = {"spvers": self.SPOOL_VERSION, "hnm": hostname, "jobOwner": owner, "jobFile": filename,
                 "jobcwd": cwd}
        if block:
            attrs["blocking"] = "spool"
        contentType = "application/tractor-spool"
        if format == "JSON":
            contentType += "-json"
        headers = {"Content-Type": contentType}
        return self._transaction(
            self.SPOOL, attrs, payload=jobData, translation=None, headers=headers, skipLogin=skipLogin)
        
    def close(self):
        """Close the connection with the engine by logging out and invalidating the session id."""
        if not self.tsid:
            # if there's no session id, then there's nothing to close
            self.dprint("no session id established.  connection considered closed.")
            return
        self.dprint("close engine connection")
        attrs = {"q": "logout", "user": self.user}
        self._transaction(self.MONITOR, attrs, translation="logout")
        # tsid is used by isOpen() method, so clear it since connection is now closed
        self.tsid = None

# a singleton engine that APIs can share
TheEngineClient = EngineClient()


def test_spool():
    """This test function provides an example of how to establish a connection with the
    engine and spool a job."""
    import tractor.api.author as author
    job = author.Job(title="a one-task job")
    task = job.newTask(title="the render task")
    task.newCommand(argv=["/bin/sleep", "10"], service="pixarRender")
    print job.asTcl()

    hostname = os.environ["TRACTOR_HOST"]
    port = int(os.environ["TRACTOR_PORT"])
    user = "adamwg"
    client = EngineClient(hostname, port, user, password="", debug=True)
    client.open()
    result = client.spool(job.asTcl())
    #result = client.spool(job.asJSON(), format="JSON")
    client.close()
    print result

if __name__=="__main__":
    test_spool()
