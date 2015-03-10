"""
This program is used to manage the starting and stopping of the 
postgresql database server that is to be used with the engine.
"""

import sys, os, subprocess, time, shutil, StringIO, ctypes, re
import tractor.base.rpg
import rpg.sql
import rpg.sql.PGFormat as PGFormat
import rpg.CmdLineTool as CmdLineTool
import rpg.OptionParser as OptionParser
import rpg.osutil as osutil
import rpg.progutil as progutil
import rpg.pathutil as pathutil
import rpg.unitutil as unitutil
import rpg.sql

import tractor.base.EngineDB as EngineDB
import tractor.base.EngineConfig as EngineConfig
import ddl
import upgrade

# Tractor-<version>/lib/psql from Tractor-<version>/lib/python2.7/site-packages/tractor/apps/dbctl/
RELATIVE_PATH_TO_INSTALL_ROOT = "../../../../../.." 
MAX_DB_CONNECT_ATTEMPTS = 10
MAX_LANGUAGE_ATTEMPTS = 10
SUPERUSER = "root"
# wait this long after starting postgresql before doing anything
POST_START_SLEEP = 2

PG_CONFIG_FILENAME = "postgresql.conf"
PG_INCLUDES_FILENAME = "includes.conf"

MIN_PURGE_YEAR = 2000
MAX_PURGE_YEAR = 2099

def setPGID():
    os.setpgid(os.getpid(), os.getpid())

class DBControlToolError(CmdLineTool.CmdLineToolError):
    pass

class ExecSQLError(DBControlToolError):
    pass

class DBControlTool(CmdLineTool.BasicCmdLineTool):
    progname = "tractor-dbctl"

    description = """
    This program is used to manage starting and stopping of the postgresql 
    database server that is used with the tractor engine.
    """

    options = [
        CmdLineTool.BooleanOption ("--start-for-engine", dest="startForEngine", help="start the postgresql server and automatically build/upgrade schema as configured in db.config (used by engine)"),
        CmdLineTool.BooleanOption ("--stop-for-engine", dest="stopForEngine", help="stop the postgresql server as configured in db.config (used by engine)"),
        CmdLineTool.BooleanOption ("--start", dest="start", help="start the postgresql server"),
        CmdLineTool.BooleanOption ("--stop", dest="stop", help="stop the postgresql server"),
        CmdLineTool.BooleanOption ("--status", dest="status", help="check the status of the postgresql server"),
        CmdLineTool.BooleanOption ("--init", dest="init", help="initialize the postgresql data directory; server must not be running"),
        CmdLineTool.BooleanOption ("--build", dest="build", help="build the tractor database; server must be running"),
        CmdLineTool.BooleanOption ("--destroy", dest="destroy", help="remove the postgres database directory; server must not be running"),
        CmdLineTool.BooleanOption ("--upgrade", dest="upgrade", help="update the database schema"),
        CmdLineTool.BooleanOption ("--check-upgrade", dest="checkUpgrade", help="check whether upgrades are required"),
        CmdLineTool.BooleanOption ("--no-auto-upgrade", dest="noAutoUpgrade", help="prevent automatic upgrading of the database schema"),
        CmdLineTool.BooleanOption ("--purge-jobs", dest="purgeJobs", help="remove all jobs"),
        CmdLineTool.StringOption ("--purge-archive-to-year-month", dest="purgeArchiveToYearMonth", help="remove archived jobs up to and including specified year-month in YY-MM format"),
        CmdLineTool.BooleanOption ("--vacuum", dest="vacuum", help="rebuild tables of non-deleted jobs to save space"),
        CmdLineTool.BooleanOption ("--reset-job-counter", dest="resetJobCounter", help="reset the job id counter so that job ids (jids) start at 1; can only be used with --purge-jobs"),
        CmdLineTool.BooleanOption ("--show-params", dest="showParams", help="show stored database paramaters"),
        CmdLineTool.BooleanOption ("--update-params", dest="updateParams", help="update database with paramaters stored in config files"),
        CmdLineTool.BooleanOption ("--tail-log", dest="tailLog", help="tail and follow the last lines of postgres message log"),
        CmdLineTool.BooleanOption ("--logs-usage", dest="logsUsage", help="report disk space used by postgresql message log files"),
        CmdLineTool.BooleanOption ("--purge-logs", dest="purgeLogs", help="remove postgresql message log files"),
        CmdLineTool.StringOption ("--backup", dest="backup", help="write backup to specified file"),
        CmdLineTool.StringOption ("--restore", dest="restore", help="restore database from specified backup file"),
        CmdLineTool.StringOption ("--config-dir", dest="configDir",
                                  help="path to tractor config directory"),
        ] + CmdLineTool.BasicCmdLineTool.options

    def __init__(self, *args, **kwargs):
        super(DBControlTool, self).__init__(*args, **kwargs)
        self.config = None
        if osutil.getlocalos() == "Linux":
            # preload libpq.so file so that later imports of psycopg2 will get the proper library
            # since rmanpy as compiled only looks in the install dir's lib/, but libpq is in lib/psql/lib
            ctypes.cdll.LoadLibrary(os.path.join(self.installDir(), "lib", "psql", "lib", "libpq.so.5"))
            # this doesn't appear to be necessary on OSX since the stock install comes with a valid libpq.so
            
    def parseArgs(self, *args, **kwargs):
        """This method gets called before execute() to validate command line arguments."""
        result = super(DBControlTool, self).parseArgs(*args, **kwargs)
        # no additional args should be supplied on the command line once flags have been removed
        if self.args:
            raise CmdLineTool.HelpRequest, self.getHelpStr()
        if self.opts.configDir:
            if not os.path.exists(self.opts.configDir):
                raise OptionParser.OptionParserError("Config dir %s does not exist" % self.opts.configDir)
        if self.opts.resetJobCounter and not self.opts.purgeJobs:
            raise OptionParser.OptionParserError("--reset-job-counter can only be used with --purge-jobs.")
        if not self.opts.configDir:
            raise OptionParser.OptionParserError("--config-dir must be specified.")
        return result

    def execute(self):
        """This method gets called automatically by CmdLineTool, and is the core logic of the program."""
        self.config = EngineConfig.EngineConfig(self.configDir(), self.installDir())
        # check that process is owned by proper user; files will be owned by that user
        processOwner = osutil.getusername()
        dataDirOwner = osutil.ownerForPath(self.config.pgDataDir())
        configuredOwner = self.config.tractorEngineOwner()
        if configuredOwner and processOwner != configuredOwner:
            raise DBControlToolError, "tractor-dbctl is configured to be run by %s; the owner of this process is %s." \
                  % (configuredOwner, processOwner)
        # check that engine owner owns the data directory
        if dataDirOwner != processOwner:
            raise DBControlToolError, "The database data dir %s is owned by %s; the owner of this process is %s." \
                  % (self.config.pgDataDir(), dataDirOwner, processOwner)

        if self.opts.status:
            self.status()
        elif self.opts.start:
            self.start()
        elif self.opts.stop:
            self.stop()
        elif self.opts.startForEngine:
            self.startForEngine()
        elif self.opts.stopForEngine:
            self.stopForEngine()
        elif self.opts.init:
            self.init()
        elif self.opts.build:
            self.build()
        elif self.opts.destroy:
            self.destroy()
        elif self.opts.checkUpgrade:
            self.checkUpgrade()
        elif self.opts.upgrade:
            self.upgrade()
        elif self.opts.purgeJobs:
            self.purgeJobs()
        elif self.opts.purgeArchiveToYearMonth:
            self.purgeArchiveToYearMonth()
        elif self.opts.vacuum:
            self.vacuum()
        elif self.opts.backup:
            self.backup()
        elif self.opts.restore:
            self.restore()
        elif self.opts.purgeLogs:
            self.purgeLogs()
        elif self.opts.logsUsage:
            self.tailLog()
        elif self.opts.tailLog:
            self.tailLog()
        elif self.opts.showParams:
            self.showParams()
        elif self.opts.updateParams:
            self.updateParams()
        else:
            raise OptionParser.OptionParserError("No operations were specified.  Use --help for options.")

    def isDebugMode(self):
        """Returns True if debug mode is turned on by way of config file or command line option."""
        return self.opts.debug or (self.config and self.config.isDbDebug())

    def installDir(self):
        """Return the full path to this tractor installation."""
        thisScriptPath = os.path.dirname(sys.argv[0])
        installDir = os.path.join(thisScriptPath, RELATIVE_PATH_TO_INSTALL_ROOT)
        installDir = os.path.realpath(installDir)
        return installDir

    def configDir(self):
        """Return the full path to the config dir."""
        return os.path.realpath(self.opts.configDir)

    def dprint(self, msg, inverse=False):
        if self.isDebugMode() and not inverse or not self.isDebugMode() and inverse:
            sys.stderr.write(msg)
            sys.stderr.write("\n")

    def log(self, msg):
        sys.stderr.write(msg)
        sys.stderr.write("\n")

    def runCommand(self, argv, input=None, errIsOut=False, **kw):
        """Run the specified command, returning the return code, stdout, and stderr."""
        self.dprint("Running %s" % argv)
        stdin = subprocess.PIPE if input is not None else None
        stderr = subprocess.STDOUT if errIsOut else subprocess.PIPE
        proc = subprocess.Popen(argv, stdin=stdin, stdout=subprocess.PIPE, stderr=stderr, **kw)
        # NOTE: http://docs.python.org/2/library/subprocess.html warns that input to Popen.communicate() shouldn't be "large"
        out, err = proc.communicate(input=input)
        rcode = proc.wait()
        self.dprint(out)
        if not errIsOut:
            self.dprint(err)
        return rcode, out, err

    def start(self):
        """Start the postgresql server."""
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "A postgresql server is already running on the data directory %s." % self.config.pgDataDir()
        self.startPostgreSQL()
        
    def stop(self):
        """Stop the postgresql server."""
        if not self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is NOT running on the data directory %s." % self.config.pgDataDir()
        self.stopPostgreSQL()
        
    def status(self):
        """Check the status of the postgresql server."""
        if self.isPostgreSQLRunning():
            self.log("A postgresql server is running on the data directory %s." % self.config.pgDataDir())
            if self.isPostgreSQLReachable():
                self.log("The postgresql server is reachable through port %d." % self.config.dbPort())
                if self.databaseExists():
                    self.log("The database %s exists." % self.config.dbDatabaseName())
                else:
                    self.log("The database %s does NOT exist." % self.config.dbDatabaseName())
            else:
                self.log("The postgresql server is NOT reachable through port %d." % self.config.dbPort())
        else:
            self.log("A postgresql server is NOT running on the data directory %s." % self.config.pgDataDir())

    def init(self):
        """Initialize the postgresql data directory."""
        # make sure postgresql server isn't already running
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must first be stopped with --stop."
        # make sure there isn't an existing postgres database
        if os.path.exists(self.config.pgDataDir()) and len(os.listdir(self.config.pgDataDir())) > 0:
            raise DBControlToolError, "%s is not an empty directory." % self.config.pgDataDir()
        self.initPostgreSQL()
        
    def build(self):
        if self.databaseExists():
            raise DBControlToolError, "Database already exists."
        self.buildDB()

    def startForEngine(self):
        """Conditionally start the postgresql server, performing any database initialization as required."""        
        # initialize db if required and configured to do so
        if not os.path.exists(self.config.pgDataDir()) or not os.path.exists(os.path.join(self.config.pgDataDir(), "postgresql.conf")):
            self.dprint("Considering db initialization because one of the following do not exist:\n%s\n%s" %
                        (self.config.pgDataDir(), os.path.join(self.config.pgDataDir(), "postgresql.conf")))
            if self.config.doDbInit():
                self.initPostgreSQL()
            else:
                raise DBControlToolError, "The postgresql data dir %s was not found and %s is not configured to create a new one." % (self.config.pgDataDir(), self.progname)

        # start postgresql server if required and configured to do so
        if self.isPostgreSQLRunning():
            if self.config.doDbUseExisting():
                self.dprint("A postgresql server is already running.  The system has been configured to use it.")
            else:
                raise DBControlToolError, "A postgresql server is already running.  Set %s to True in %s to use an existing postgresql server." \
                    % (EngineConfig.DB_USE_EXISTING, self.config.dbSiteConfigFilename())
        else:
            if self.config.doDbStartup():
                self.startPostgreSQL()
                time.sleep(POST_START_SLEEP)
                if not self.isPostgreSQLRunning(maxAttempts=MAX_DB_CONNECT_ATTEMPTS):
                    raise DBControlToolError, "Failed to start a postgresql server on data directory %s.  A different server may be already running; to check, try 'ps -elf | grep postgres'.  Or another service is using port %d; to check, try 'sudo fuser %d/tcp'.\%s may have more info." \
                          % (self.config.pgDataDir(), self.config.dbPort(), self.config.dbPort(), self.config.pgLogFilename())
            else:
                raise DBControlToolError, "A postgresql server was not started because %s is set to False in %s to prevent the automatic starting of a postgresql server.  Change this setting to True, or manually ensure your custom postgresql server is running and set %s to True." \
                    % (EngineConfig.DB_STARTUP, self.config.dbSiteConfigFilename(), EngineConfig.DB_USE_EXISTING)

        # test if reachable
        if not self.isPostgreSQLReachable(maxAttempts=MAX_DB_CONNECT_ATTEMPTS):
            raise DBControlToolError, "Unable to connect to postgresql server on port %d.  Check %s for more info." % (self.config.dbPort(), self.config.pgLogFilename())

        # build database
        if not self.databaseExists():
            self.buildDB()
        else:
            self.dprint("Database %s exists.  Building database is not required." % self.config.dbDatabaseName())

        # ensure that languages are loaded
        if not self.isLanguageReady("plpython2u"):
            raise DBControlToolError, "Unable to verify that plpython2u has been loaded."
        if not self.isLanguageReady("plpgsql"):
            raise DBControlToolError, "Unable to verify that plpgsql has been loaded."

        # see if an upgrade is required
        if self.config.doDbAutoUpgrade() and not self.opts.noAutoUpgrade:
            upgrades = self.getUpgrades()
            if upgrades:
                self.upgradeDB(upgrades)

    def stopForEngine(self):
        """Stop the postgresql server."""
        if self.config.doDbShutdown() and self.isPostgreSQLRunning():
            self.stopPostgreSQL()

    def destroy(self):
        """This method removes the existing a new postgresql database directory.
        There must be no existing postgresql server running."""

        # make sure postgresql server is not running
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must be shutdown with --stop before using --destroy."

        # make sure there isn't an existing postgres database
        if not os.path.exists(self.config.pgDataDir()):
            raise DBControlToolError, "%s does not exist." % self.config.pgDataDir()

        # remove the contents of the data directory
        self.log("Removing contents of data directory %s." % self.config.pgDataDir())
        for filename in os.listdir(self.config.pgDataDir()):
            fullFilename = os.path.join(self.config.pgDataDir(), filename)
            try:
                if os.path.isfile(fullFilename):
                    os.remove(fullFilename)
                else:
                    shutil.rmtree(os.path.join(self.config.pgDataDir(), fullFilename))
            except (IOError, OSError), err:
                self.log("Unable to remove %s: %s" % (filename, str(err)))

    def checkUpgrade(self):
        """This method reports what upgrades are required."""
        # make sure postgresql server is not running
        if not self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is not running.  It may be started with --start."

        # determine the installed schema version
        installedVersion = self.installedSchemaVersion()
        # get the required upgrade actions
        upgrades = self.getUpgrades()
        if not upgrades:
            self.log("No upgrades are required to be compatible with schema version %s." 
                     % upgrade.SCHEMA_VERSION)
        else:
            self.log("The following upgrades need to be applied to upgrade the schema from version %s to %s:"
                     % (installedVersion, upgrade.SCHEMA_VERSION))
            for u in upgrades:
                self.log(str(u))

    def upgrade(self):
        """This method upgrades the database schema through a high-level schema upgrade interface."""
        # make sure postgresql server is not running
        if not self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is not running.  It may be started with --start."
        # get the required upgrades
        upgrades = self.getUpgrades()
        if not upgrades:
            self.log("The database schema is current.")
            return

        # perform upgrade
        self.upgradeDB(upgrades)

    def upgradeDB(self, upgrades):
        """This method executes the upgrade actions."""
        self.log("Upgrading database schema.")
        db = EngineDB.EngineDB(user=SUPERUSER, db=self.config.dbDatabaseName(),
                               dbhost=self.config.dbHostname(), port=self.config.dbPort())
        db.open()
        db._execute("begin")
        i = 0
        for u in upgrades:
            i += 1
            self.log("Applying upgrade %d of %d: %s" % (i, len(upgrades), str(u)))
            db._execute(u.getSQL())
        db._execute("UPDATE param SET value='%s' WHERE name='schema-version'" % upgrade.SCHEMA_VERSION)
        db._execute("end")

    def getUpgrades(self):
        """This method returns the upgrade actions to upgrade the database schema.  It can also be used
        to simply test whether the schema is current."""
        
        # determine the installed schema version
        installedVersion = self.installedSchemaVersion()

        # exit with error if installed schema version is not known; db record may have become corrupted
        # note that this check may need to be altered if we plan to support downgrades since
        # a newer version could leave a value in the db that is not in the SCHEMA_VERSIONS list
        # of an older version
        if installedVersion not in upgrade.SCHEMA_VERSIONS:
            raise DBControlToolError, "Installed version '%s' is not in list of valid versions: %s" \
                  % (installedVersion, upgrade.SCHEMA_VERSIONS)

        # exit with error if target schema version is not known; this is a developer bug for
        # not maintaining constants properly in upgrade.py
        if upgrade.SCHEMA_VERSION not in upgrade.SCHEMA_VERSIONS:
            raise DBControlToolError, "SCHEMA_VERSION '%s' is not in list of valid versions: %s." \
                  % (upgrade.SCHEMA_VERSION, upgrade.SCHEMA_VERSIONS)

        # get the upgrades required to get to current version
        upgrades = upgrade.getUpgrades(installedVersion, upgrade.SCHEMA_VERSION)
        return upgrades

    def initPostgreSQL(self):
        """Initialize the postgresql database."""
        argv = [os.path.join(self.config.pgBinDir(), "initdb"), "-D", self.config.pgDataDir(), "--username", SUPERUSER]
        self.log("Initializing postgres database directory %s." % self.config.pgDataDir())
        rcode, out, err = self.runCommand(argv, errIsOut=True)
        if rcode:
            self.dprint(out, inverse=True)
            raise DBControlToolError, "Failed to initialize database with %s" % str(argv)
        
        # append directive to include file that will be contain other include directives pointing to configuration
        # override files in install dir and config dir; that include file gets rewritten on each startup
        pgConfigFilename = os.path.join(self.config.pgDataDir(), PG_CONFIG_FILENAME)
        pgIncludesFilename = os.path.join(self.config.pgDataDir(), PG_INCLUDES_FILENAME)
        try:
            f = open(pgConfigFilename, "a")
            f.write("include_if_exists='%s'\n" % pgIncludesFilename)
            f.close()
        except (IOError, OSError), err:
            raise DBControlToolError, "Unable to write to %s: %s" % (pgConfigFilename, str(err))

    def databaseExists(self):
        """Returns True if database already exists."""
        psql = os.path.join(self.config.pgBinDir(), "psql")
        argv = [psql, "-h", "localhost", "-U", SUPERUSER,  "-d", "postgres", "-p", str(self.config.dbPort()), "--tuples-only", "--no-align", "-c", "SELECT 1 from pg_database WHERE datname='%s'" % self.config.dbDatabaseName()]
        rcode, out, err = self.runCommand(argv)
        if rcode:
            raise DBControlToolError, "Test for existence of database failed."
        out = out.strip()
        return out == "1"

    def buildDB(self):
        """Create the tables, functions, and users."""
        self.log("Building tractor database.")

        psql = os.path.join(self.config.pgBinDir(), "psql")
        argv = [psql, "-h", "localhost", "-U", SUPERUSER, "-d", "postgres", "-p", str(self.config.dbPort())]
        rcode, out, err = self.runCommand(argv, input=ddl.ddl(), errIsOut=True)
        if rcode:
            # ddl injection output would have already been displayed above in debug mode
            self.dprint(out, inverse=True)
            raise DBControlToolError, "Failed to build database with ddl input to %s" % str(argv)

    def createIncludeConfigFile(self):
        """Create the config file to point to config/install dir postgresql overrides."""
        pgIncludesFilename = os.path.join(self.config.pgDataDir(), PG_INCLUDES_FILENAME)
        tractorOverridesFilename = os.path.join(self.installDir(), "config", PG_CONFIG_FILENAME)
        siteOverridesFilename = os.path.join(self.configDir(), PG_CONFIG_FILENAME)
        try:
            f = open(pgIncludesFilename, "w")
            f.write("include_if_exists='%s'\n" % tractorOverridesFilename)
            f.write("include_if_exists='%s'\n" % siteOverridesFilename)
            f.close()
        except (IOError, OSError), err:
            raise DBControlToolError, "Unable to write to %s: %s" % (pgIncludesFilename, str(err))
        
    def startPostgreSQL(self):
        """Issue command to start the postgresql server."""
        self.createIncludeConfigFile()
        if self.config.dbHostname() != "localhost":
            raise DBControlToolError, "DBHost must be set to localhost in %s" % self.config.dbSiteConfigFilename()
        argv = [
            os.path.join(self.config.pgBinDir(), "pg_ctl"), "-D", self.config.pgDataDir(),
            "-o", "-p %d" % self.config.dbPort(),
            "-l", self.config.pgLogFilename(), "start"]
        self.log("Starting postgres server on the data directory %s and on port %d." % (self.config.pgDataDir(), self.config.dbPort()))
        # PYTHONHOME is required so that postgres server can locate python libraries for plpython functions
        rcode, out, err = self.runCommand(
            argv, errIsOut=True, close_fds=True, preexec_fn=setPGID,
            env={"PYTHONHOME": self.installDir()})
        if rcode:
            self.dprint(out, inverse=True)
            raise DBControlToolError, "Failed to start postgresql server with %s" % " ".join(argv)

    def stopPostgreSQL(self):
        """Issue the command to stop the postgresql server."""
        if self.config.dbHostname() != "localhost":
            raise DBControlToolError, "DBHost must be set to localhost in %s" % self.config.dbSiteConfigFilename()
        argv = [os.path.join(self.config.pgBinDir(), "pg_ctl"), "-D", self.config.pgDataDir(), "stop", "-m", "fast"]
        self.log("Stopping postgres server on data directory %s with %s" % (self.config.pgDataDir(), " ".join(argv)))
        rcode, out, err = self.runCommand(argv, errIsOut=True)
        if rcode:
            self.dprint(out, inverse=True)
            raise DBControlToolError, "Failed to stop postgresql server with %s" % " ".join(argv)

    def isPostgreSQLRunning(self, maxAttempts=1):
        """Returns True if the postgresql server is running on the configured data directory."""
        argv = [os.path.join(self.config.pgBinDir(), "pg_ctl"), "-D", self.config.pgDataDir(), "status"]
        for i in range(maxAttempts):
            self.dprint("Checking postgresql server status.")
            rcode, out, err = self.runCommand(argv)
            if rcode == 0 and "server is running" in out:
                return True
            self.dprint(out + err);
            time.sleep(1)
        return False

    def isPostgreSQLReachable(self, maxAttempts=1):
        """Returns True if the postgresql server is reachable."""
        # heck that a connection can be made
        for i in range(maxAttempts):
            self.dprint("Checking postgresql connectivity.")
            try:
                result = self.execSQLWithResult("SELECT 1", dbname="postgres")
                return True
            except ExecSQLError, err:
                self.dprint("isPostgreSQLReachable(): %s" % str(err))
                time.sleep(1)
                pass
        # no connection could be made
        return False

    def isLanguageReady(self, language):
        """Returns True if the plpython language has been loaded by the postgresql server."""
        query = "SELECT EXISTS (SELECT 1  FROM   pg_language  WHERE  lanname = '%s')" % language
        for i in range(MAX_LANGUAGE_ATTEMPTS):
            self.dprint("Checking postgresql for existence of %s: %s" % (language, query))
            result = self.execSQLWithResult(query)
            if result[0][0]:
                return True
            self.log("%s not loaded.  Trying again." % language)
            time.sleep(1)
        return False

    def purgeJobs(self):
        """Truncate and/or drop tables to purge records and free disk space."""
        self.log("Purging.")
        ok = self.execSQL("SELECT TractorPurgeArchive()", user=ddl.TABLE_OWNER)
        if not ok:
            raise DBControlToolError, "Failed to purge archive data."
        ok = self.execSQL("SELECT TractorPurgeLive()", user=ddl.TABLE_OWNER)
        if not ok:
            raise DBControlToolError, "Failed to purge live data."
        if self.opts.resetJobCounter:
            ok = self.execSQL("SELECT TractorResetJobCounter()")
            if not ok:
                raise DBControlToolError, "Failed to reset job counter."
                
    def purgeArchiveToYearMonth(self):
        """Truncate and/or drop tables to purge records and free disk space."""
        # purge target year and month must be specified in YYYY-MM or YY-MM format
        matches = re.findall("^((\d\d)?(\d\d))-((\d)?(\d)$)", self.opts.purgeArchiveToYearMonth)
        if not matches:
            raise OptionParser.OptionParserError(
                "Purge target year and month must be specified as YYYY-MM or YY-MM.")
        year = matches[0][0]
        if len(year) == 2:
            year = "20" + year
        year = int(year)
        # sanity check year
        if year < MIN_PURGE_YEAR or year > MAX_PURGE_YEAR:
            raise OptionParser.OptionParserError("Purge target year must be between %d and %d." %
                                                 (MIN_PURGE_YEAR, MAX_PURGE_YEAR))
        # validate month
        month = int(matches [0][3])
        if month < 1 or month > 12:
            raise OptionParser.OptionParserError("Purge target month must be between 1 and 12.")
        # perform purge
        self.log("Purging archive to %d-%02d." % (year, month))
        ok = self.execSQL("SELECT TractorPurgeArchiveToYearMonth(%d, %d)" % (year, month))
        if not ok:
            raise DBControlToolError, "Failed to purge archive data."

    def vacuum(self):
        """Rebuild active tables to free space."""
        # make sure postgresql server is not running so that vacuum can manage its own starting and stopping of the postgresql server
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must be shutdown with --stop before using --vacuum."
        # start postgresql
        self.startPostgreSQL()
        time.sleep(POST_START_SLEEP)
        # check that starting postgresql was successful
        if not self.isPostgreSQLRunning(maxAttempts=MAX_DB_CONNECT_ATTEMPTS):
            raise DBControlToolError, "Failed to start a postgresql server on data directory %s.  A different server may be already running; to check, try 'ps -elf | grep postgres'.  Or another service is using port %d; to check, try 'sudo fuser %d/tcp'.\%s may have more info." \
                  % (self.config.pgDataDir(), self.config.dbPort(), self.config.dbPort(), self.config.pgLogFilename())
        # ensure it is reachable before continuing
        if not self.isPostgreSQLReachable():
            raise DBControlToolError, "Unable to connect to postgresql server on port %d.  Check %s for more info." % (self.config.dbPort(), self.config.pgLogFilename())
        # perform vacuum
        self.log("Vacuuming.")
        ok = self.execSQL("VACUUM FULL")
        if not ok:
            self.stopPostgreSQL()
            raise DBControlToolError, "Failed to vacuum."
        # stop postgresql
        self.stopPostgreSQL()
                
    def backup(self):
        """Create a backup of the database to a specified file."""
        # make sure postgresql server is not running
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must be shutdown with --stop before using --backup."

        self.log("Backing up data directory %s" % self.config.pgDataDir())
        dataParent, dataChild = os.path.split(self.config.pgDataDir())
        argv = ["tar", "cfz", self.opts.backup,  "-C", dataParent, dataChild]
        rcode, out, err = self.runCommand(argv)
        if out:
            self.log(out)
        if err:
            self.log(err)
        if err:
            raise DBControlToolError, "Backup failed."
        
    def restore(self):
        """Restore the database from a backup of a specified file."""
        # make sure postgresql server is not running
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must be shutdown with --stop before using --restore."

        # make sure there isn't an existing postgres database
        if os.path.exists(self.config.pgDataDir()) and len(os.listdir(self.config.pgDataDir())) > 0:
            raise DBControlToolError, "%s is not an empty directory." % self.config.pgDataDir()

        self.log("Restoring to data directory %s" % self.config.pgDataDir())
        dataParent, dataChild = os.path.split(self.config.pgDataDir())
        argv = ["tar",  "xfz", self.opts.restore, "-C", dataParent]
        rcode, out, err = self.runCommand(argv)
        if out:
            self.log(out)
        if err:
            self.log(err)
        if err:
            raise DBControlToolError, "Restore failed."

    def tailLog(self):
        """Tail and follow the most recent postgresql message log file."""
        logFile = os.path.join(self.config.pgDataDir(), "pg_log", "log.%s.csv" % time.strftime("%w"))
        argv = ["tail", "-f", logFile]
        print " ".join(argv)
        sys.stdout.flush()
        os.system(" ".join(argv))
                
    def logsUsage(self):
        """Report the disk space used by postgresql logs."""
        logDir = os.path.join(self.config.pgDataDir(), "pg_log")
        usage = 0
        try:
            for f in os.listdir(logDir):
                logFilename = os.path.join(logDir, f)
                usage += os.path.getsize(logFilename) if os.path.isfile(logFilename) else 0
        except (IOError, OSError), err:
            raise DBControlToolError, "Unable to determine disk usage of log directory %s: %s" % (logDir, str(err))
        usageStr = unitutil.formatBytes(usage)
        usageStr += "B" if usageStr[-1] != "B" else ""
        print "The postgresql log directory %s is taking %s of disk space." % (logDir, usageStr)
        
    def purgeLogs(self):
        """Remove all postgresql log files."""
        # make sure postgresql server is not running
        if self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is already running.  It must be shutdown with --stop before using --purge-logs."
        logDir = os.path.join(self.config.pgDataDir(), "pg_log")
        try:
            shutil.rmtree(logDir)
        except (IOError, OSError), err:
            raise DBControlToolError, "Unable to remove log directory %s: %s" % (logDir, str(err))
                
    def showParams(self):
        """Display certain parameters stored in database."""
        if not self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is not running.  It may be started with --start."
        result = self.execSQLWithResult("SELECT name,value FROM param")
        for row in result:
            print "%s = %s" % (row[0], row[1])

    def updateParams(self):
        """Push certain parameters stored in config files to database."""
        if not self.isPostgreSQLRunning():
            raise DBControlToolError, "The postgresql server is not running.  It may be started with --start."
        archiving = 1 if self.config.doDbArchiving() else 0
        ok = self.execSQL("SELECT TractorToggleArchiving(%d)" % archiving)
        if not ok:
            raise DBControlToolError, "Failed to push DBArchiving setting."
        self.log("Turned archiving %s." % ("on" if archiving else "off"))

    def installedSchemaVersion(self):
        """Returns the version of the currently installed schema."""
        result = self.execSQLWithResult("SELECT value FROM param WHERE name='schema-version'")
        if result:
            return result[0][0]
        
    def execSQL(self, sql, user=SUPERUSER, dbname=None, *args):
        """Execute the supplied sql statement using psql.  Returns True if return code is 0.  Note that
        return code may still be 0, even when executed SQL may not have expected effect."""
        dbname = dbname or self.config.dbDatabaseName()
        psql = os.path.join(self.config.pgBinDir(), "psql")
        argv = [psql, "-h", "localhost", "-U", user, "-d", dbname, "-p", str(self.config.dbPort()), "-c", sql] + list(args)
        rcode, out, err = self.runCommand(argv)
        return rcode == 0
        if rcode:
            raise ExecSQLError, "execSQL(): problem executing '%s': %s" % (sql, str(err))
        return out

    def execSQLWithResult(self, sql, user=SUPERUSER, dbname=None, *args):
        """Execute the supplied sql statement using the EngineDB's client."""
        dbname = dbname or self.config.dbDatabaseName()
        db = EngineDB.EngineDB(user=user, db=dbname, dbhost=self.config.dbHostname(), port=self.config.dbPort())
        try:
            db.open()
            db._execute(sql)
            result = db.cursor.fetchall()
            db.close()
        except rpg.sql.SQLError, err:
            raise ExecSQLError("execSQLWithResult(): problem running '%s' as '%s' with args '%s': %s" % (sql, user, str(args), err))
        return result


def main():
    try:
        return DBControlTool(lock=True).run()
    except (CmdLineTool.CmdLineToolError, OptionParser.OptionParserError, EngineConfig.EngineConfigError) , err:
        print >>sys.stderr, err
        return 2

if __name__ == '__main__':
    sys.exit(main())
