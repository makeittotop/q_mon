#
# dbstaging -- an internal module for staging queued jobs to the database.
#
# ____________________________________________________________________ 
# Copyright (C) 2013-2014 Pixar Animation Studios. All rights reserved.
#
# The information in this file is provided for the exclusive use of the
# software licensees of Pixar.  It is UNPUBLISHED PROPRIETARY SOURCE CODE
# of Pixar Animation Studios; the contents of this file may not be disclosed
# to third parties, copied or duplicated in any form, in whole or in part,
# without the prior written permission of Pixar Animation Studios.
# Use of copyright notice is precautionary and does not imply publication.
#
# PIXAR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT
# SHALL PIXAR BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES
# OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
# WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
# ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
# ____________________________________________________________________ 
#

import os, sys, time, datetime, argparse, json, StringIO, ctypes

def installDir():
    """Return the full path to this tractor installation."""
    RELATIVE_PATH_TO_INSTALL_ROOT = "../../../../../.." 
    thisModulePath = os.path.dirname(__file__)
    installDir = os.path.join(thisModulePath, RELATIVE_PATH_TO_INSTALL_ROOT)
    installDir = os.path.realpath(installDir)
    return installDir

import tractor.base.EngineDB as EngineDB
import tractor.base.rpg.sql.PGFormat as PGFormat
import _alfparse

## ------------------------------------------------------------- ##
class PlpyCursor(object):
    """This class follows the part of the psycopg2 protocol
    so that this tool can be used as a command line tool
    using psycopg2, or called from within a plpython function."""
    def __init__(self, plpyModule):
        self.plpy = plpyModule
        self.cachedResult = None

    def execute(self, query):
        # the plpy function is already a transaction, so running a commit
        # now would assume a subtransaction and cause an SPI_EXCEPTION.
        # so let's ignore such commits for now until we are sure we want
        # to deal with subtransactions
        if query.lower() == "commit":
            return
        # cache the result of the query for use by fetchall()
        if type(query) is unicode:
            self.cachedResult = self.plpy.execute(query.encode('utf8'))
        else:
            self.cachedResult = self.plpy.execute(query)
        
    def fetchall(self):
        result = self.cachedResult
        self.cachedResult = None
        return result


## ------------------------------------------------------------- ##
def main (argStr=None, plpyModule=None):
    rc = 1
    msg = "unknown job handling failure"
    args = None

    try:
        parser = argparse.ArgumentParser(prog="dbstaging")

        parser.add_argument("jobfile",
                            help="name of the engine-internal incoming file")

        parser.add_argument("--user", default="nobody", dest="jobOwner",
                            help="original spool client's job owner")

        parser.add_argument("--hname", default="localhost",
                            help="original spool client's host name")

        parser.add_argument("--haddr", default="127.0.0.1",
                            help="original spool client's host address")

        parser.add_argument("--jid", default="",
                            help="the jid preassigned by the engine")

        parser.add_argument("--expandctx", default=None,
                            help="indicates that job file is actually an " \
                                 "expand snippet, and that it should use " \
                                 "the given tid as the base parent task-id," \
                                 "and inherit the given serialsubtasks state," \
                                 "usage --expandctx=jid.tid.cid.crev.sst")

        parser.add_argument("--spooltime", dest="spoolTime",
                            default=str(int(time.time())),
                            help="the timestamp assigned by the engine")

        parser.set_defaults(infmt="ALFRED")
        parser.add_argument("--in-json", dest="infmt",
                            action="store_const", const="JSON")
        parser.add_argument("--in-alf", dest="infmt",
                            action="store_const", const="ALFRED")

        parser.add_argument("--remove", default=False, action="store_true")
        parser.add_argument("--trace", default=False, action="store_true")
        parser.add_argument("--dbgmode", default=None)

        ## ------- ##
        # Pass-through args when handling deprecated delivery of alfred
        # scripts from older clients (new clients parse the alfred scripts
        # themselves and deliver only json here).
        parser.add_argument("--userfile", default=None)
        parser.add_argument("--jobcwd", default=None)
        parser.add_argument("--priority", default="default")
        parser.add_argument("--projects", default="default")
        parser.add_argument("--tier", default="default")
        parser.add_argument("--maxactive", default=0, type=int)
        parser.add_argument("--paused", default=0, type=int)
        parser.add_argument("--svckey", default=None)
        parser.add_argument("--envkey", default=None)
        parser.add_argument("--aftertime", default=None)
        parser.add_argument("--afterjid", default=None)
        parser.add_argument("--remoteclean", default=None)
        parser.add_argument("--alfescape", default=False, action="store_true")
        ## ------- ##

        args = parser.parse_args(argStr)

        if args.infmt.upper() == "JSON":
            f = open(args.jobfile, "r")
            jobjson = f.read()
            f.close()
            # strip off the checkpoint header, if any
            if jobjson and '#'==jobjson[0] and '!'==jobjson[1]:
                k = jobjson.find('\n{')  # here's a } for stupid text editors
                if k > 0:
                    jobjson = jobjson[k+1:]
        else:
            # deprecated usage - some old spooler sent an alf-fmt file
            jobjson = _alfparse.ParseAlfJob( args, args.jobfile, None )

        jtree = json.loads( jobjson )  # parse the json!

        #
        # Validate that we are (nominally) receiving a job format
        # that we understand. There may eventually be several supported
        # formats, each requiring handler variations below.
        #
        eps = .00001
        jobfmt = "1.0"
        if "TractorJob" in jtree:
            jobfmt = jtree['TractorJob']
        try:
            ffmt = float(jobfmt)
        except:
            ffmt = 9999.99
        if ffmt > 2.0 + eps:
            raise Exception("unrecognized JSON job format: "+jobfmt)
        
        # when we are launched by tractor-engine, it places the correct
        # database login credentials on stdin for us to read
        credsrc = None if args.dbgmode else sys.stdin

        stager = SpoolingStager( credsrc, args.dbgmode, args.spoolTime,
                                 plpyModule=plpyModule )

        if args.expandctx:
            # expand handling
            rc, msg = stager.processExpand( args.jid, args.expandctx,
                                            args.jobOwner, args.haddr,
                                            jtree, jobjson,
                                            os.path.dirname(args.jobfile) )
        else:
            # initial job spool
            rc, msg = stager.processJobTree( args.jid, args.jobOwner,
                                             args.haddr, jtree )

        # success!  clean up incoming file
        if 0==rc and args.remove:
            os.unlink( args.jobfile )

    except:
        rc = 1
        errclass, excobj = sys.exc_info()[:2]
        msg = "job db staging: " + errclass.__name__ + "\n" + str(excobj)
        if hasattr(args, "trace") and args.trace:
            import traceback
            msg += "\n" + traceback.format_exc()

    if plpyModule:
        return rc, msg
    else:
        print msg  # external caller reads our stdout for data
        sys.exit(rc)


## ------------------------------------------------------------- ##
class SpoolingStager (object):
    '''
    Manage the conversion of a single-string JSON job tree
    representation into bulk-insert psql client operations.
    '''

    def __init__ (self, credentialsChannel, dbgmode=None,
                    spoolTime=None, plpyModule=None):

        self.engineDB = EngineDB.EngineDB()
        self.dbgmode = dbgmode
        self.jobObj = []
        self.allCmds = []
        self.allTasks = []
        self.instanceMap = {}
        self.nready = 0
        self.jid = 0
        self.lastTID = 0
        self.lastCID = 0
        self.tidOffset = 0
        if spoolTime == None:
            spoolTime = time.time()
        self.spooltime = datetime.datetime.fromtimestamp( float(spoolTime) )

        if plpyModule:
            self.dbconn = None
            self.dbcursor = PlpyCursor(plpyModule)
        else:
            if credentialsChannel:
                try:
                    # open connection to psql
                    import tractor.base.rpg.osutil as osutil
                    if osutil.getlocalos() == "Linux":
                        # preload libpq.so file so that later imports of psycopg2 will get the proper library
                        # since rmanpy as compiled only looks in the install dir's lib/, but libpq is in lib/psql/lib
                        ctypes.cdll.LoadLibrary(os.path.join(installDir(), "lib", "psql", "lib", "libpq.so.5"))
                        # this doesn't appear to be necessary on OSX since the stock install comes with a valid libpq.so
                    import psycopg2
                    dbConnInfo = credentialsChannel.read()
                    self.dbconn = psycopg2.connect( dbConnInfo )
                    self.dbcursor = self.dbconn.cursor()
                except:
                    raise
            else:
                print >>sys.stderr, "note: no database credentials provided."
                self.dbcursor = False


    def processJobTree (self, jid, owner, clientAddr, jtree):
        #
        # Rather than making sql insert calls for each task/cmd
        # individually as we walk the job tree, we collect long
        # lists of task data and command data, then bulk insert.
        #

        # extract the top-level job attributes (required to exist)
        self.jid = jid
        jobhdr = jtree["data"]
        task0 = self.newTask( jid, 0, jobhdr.get("serialsubtasks",0) )

        # flatten the tree of subtasks, and their commands
        if "children" in jtree:
            self.taskDescent( jtree["children"], task0, False )

        # and append any (unusual) job-level commands
        if "commands" in jobhdr:
            for c in jobhdr["commands"]:
                self.addCmd( c, task0 )

        # add the job data, with task counts now accumulated
        self.jobObj = self.addJob(jobhdr, clientAddr, task0)

        # another pass to resolve Instances (including alfred-style fwd refs)
        self.resolveTaskIntances()

        # now actually bulk insert each serialized list by type
        if self.dbgmode:
            print [self.tWalk(k) for k in task0.kids]
        elif self.dbcursor:
            self.bulkInsertJobData()

        url =  "q=load&ldtype=n&pid=" + str(os.getpid())
        url += "&spooladdr=" + clientAddr
        url += "&jid=" + str(jid)
        url += "&owner=" + owner
        return (0, url)


    def processExpand (self, jid, xpctx, jobOwner, haddr, subtree,
                        jsontxt, spooldir):
        #
        # Expect an array of subtree tasks here:
        #   [ {"data": {}, "children": []}, {...}, {...} ]
        # to be grafted in as new subtask(s) of existing task "xptid"
        #
        try:
            # parse out the inbound expand contxt info:
            #  xptid = the parent tid for these new subtasks
            #  psst  = the expanding parent's serialsubtasks state
            jid, xptid, xpcid, xpcrev, psst = xpctx.split('.')
            xptid = int(xptid)
            psst  = int(psst)
        except:
            raise Exception("invalid expand context parameter")

        task0 = self.newTask( jid, -xptid, psst )
        self.jid = jid
        self.taskDescent( subtree, task0, (psst==1) )

        # now correct the tid / cid values from the expand snippet to
        # fit into the existing values of the parent job
        self.adjustExpandIDs()

        # now bulk insert each serialized list by type
        self.bulkInsertExpandData()

        # Now generate local json for the "job diff" that the running
        # engine can apply to the live job.  This will be nearly identical
        # to the inbound expand snippet json, but it will have the new
        # tid/cid offsets applied, and the cmd text is split out into a
        # list of cmds.

        xtasks = [self.tWalk(k) for k in task0.kids]
        xcmds = {}
        maxcid = 0
        for c in self.allCmds:
            xcmds["C"+str(c.cid)] = self.cxFmt( c )
            if c.cid > maxcid: maxcid = c.cid

        x = os.path.join(spooldir, "_xj.%s.%s.%s.%s" % (jid,xptid,xpcid,xpcrev))
        f = open(x, "wb")
        f.write("{\n\"xtasks\": ")
        json.dump( xtasks, f, indent=3 )
        f.write(",\n\n\"xcmds\": ")
        json.dump( xcmds, f, indent=3 )
        f.write("\n\n}\n")
        f.close()

        url =  "q=expanded&ldtype=x&pid=" + str(os.getpid())
        url += "&owner=" + jobOwner
        url += "&spooladdr=" + haddr
        url += "&jid=" + str(jid)
        url += "&tid=" + str(xptid)
        url += "&cid=" + str(xpcid)
        url += "&rev=" + str(xpcrev)
        url += "&maxcid=" + str(maxcid)
        return (0, url)


    def taskDescent (self, tlist, ptask, serialWait):
        cmdsAny = False

        for t in tlist:

            td = t.get("data", {})

            xt = self.addTask( td, ptask.tid )

            ptask.kids.append( xt.idx )

            cmdsHere  = False
            cmdsBelow = False
            if "commands" in td:
                for c in td["commands"]:
                    self.addCmd( c, xt )
                    cmdsHere = True  # if-any

            if "children" in t:
                cmdsBelow = self.taskDescent( t["children"], xt, serialWait )

            if cmdsHere or cmdsBelow:
                cmdsAny = True  # if-any

            if cmdsHere and (not cmdsBelow) and (not serialWait):
                # this task is a leaf
                xt.state = EngineDB.STATE_READY
                xt.readytime = self.spooltime
                self.nready += 1

            if ptask.serialsubtasks:
                # if-any, after first sibling
                serialWait = True

        return cmdsAny


    def addJob (self, dct, spooladdr, task0):
        job = EngineDB.Job()
        job.jid = self.jid
        job.spooladdr = spooladdr
        job.spooltime = self.spooltime
        job.owner = dct.get("owner", "")
        job.spoolhost = dct.get("spoolhost", "")
        job.spoolfile = dct.get("spoolfile", "")
        job.spoolcwd = dct.get("spoolcwd", "")
        job.title = dct.get("title", "")
        job.priority = dct.get("priority", 0)
        if job.priority < 0:
            job.priority = abs(job.priority + 1)  # also acct for T1-style shift
            # currently the pausetime is not stored, so just set it
            # to the spooltime for now
            job.pausetime = job.spooltime
        if 0 != dct.get("paused", 0):
            job.pausetime = job.spooltime
        job.crews = dct.get("crews", [])
        job.maxactive = dct.get("maxactive", 0)
        job.tags = dct.get("tags", "").split()
        job.service = dct.get("service", "")
        job.envkey = dct.get("envkey", [])
        job.serialsubtasks = task0.serialsubtasks
        job.editpolicy = dct.get("editpolicy", "")
        job.projects = dct.get("projects", [])
        job.tier = dct.get("tier", "")
        job.minslots = dct.get("minslots")
        job.maxslots = dct.get("maxslots")
        job.metadata = dct.get("metadata", "")
        job.comment = dct.get("comment", "")
        job.etalevel = dct.get("etalevel", 1)
        job.dirmap = dct.get("dirmaps", [])
        job.afterjids = dct.get("afterjids", [])
        if dct.has_key("aftertime"):
            job.aftertime = datetime.datetime.fromtimestamp(dct.get("aftertime"))
        job.numtasks = len( self.allTasks )
        job.numready = self.nready
        job.numblocked = job.numtasks - job.numready
        job.maxcid = self.lastCID
        job.maxtid = self.lastTID

        return job


    def newTask (self, jid, tid, sst):
        task = EngineDB.Task()
        task.jid = jid
        task.tid = tid
        task.state = EngineDB.STATE_BLOCKED
        task.cids = []
        task.serialsubtasks = True if sst else False  # sst may be numeric
        task.statetime = self.spooltime
        setattr(task, "kids", []) # for construction use, not recorded to db
        setattr(task, "idx", 0) # for construction use, not recorded to db
        if tid > 0:
            # tid 0 is special and "reserved" for the job itself
            # commands attached to the job object are placed into
            # the db with their tid=0, but the task itself is not.
            task.idx = len(self.allTasks)
            self.allTasks.append( task )
        return task

    def addTask (self, dct, ptid):
        self.lastTID += 1
        task = self.newTask( self.jid, self.lastTID,
                             dct.get("serialsubtasks",0) )
        task.id = dct.get("id")
        task.title = dct.get("title")
        task.service = dct.get("service")
        task.minslots = dct.get("minslots")
        task.maxslots = dct.get("maxslots")
        task.preview = dct.get("preview", "")
        task.chaser = dct.get("chaser", "")
        task.resumeblock = dct.get("resumeblock", 0)
        task.ptids = [ptid]
        setattr(task, "ants", dct.get("ants", [])) # parsing use, not sent to db
        self.instanceMap[task.id] = task.tid
        self.instanceMap[task.title] = task.tid
        return task


    def addCmd (self, dct, ptask):
        self.lastCID += 1
        cmd = EngineDB.Command()
        cmd.jid = self.jid
        cmd.tid = ptask.tid
        cmd.cid = self.lastCID
        ptask.cids.append( cmd.cid )
        cmd.argv = dct.get("argv", [])
        cmd.msg = dct.get("msg")
        cmd.service = dct.get("service")
        cmd.tags = dct.get("tags", "").split()
        cmd.id = dct.get("id")
        cmd.refersto = dct.get("refersto")
        cmd.minslots = dct.get("minslots")
        cmd.maxslots = dct.get("maxslots")
        cmd.envkey = dct.get("envkey", [])
        cmd.retryrcodes = dct.get("retryrc", [])
        cmd.resumewhile = dct.get("resumewhile", [])
        cmd.resumepin = dct.get("resumepin", 0)

        t = dct.get("type", "RC")
        n = len(t)
        cmd.local = (n > 0 and t[0] == "L")
        cmd.expand = (n > 2 and t[2] == "X")
        if n > 1:
            if t[1] == "D":
                cmd.runtype = "cleanup"
            elif t[1] == "P":
                if n > 2 and t[2]=="D":
                    cmd.runtype = "post_done"
                elif n > 2 and t[2]=="E":
                    cmd.runtype = "post_error"
                else:
                    cmd.runtype = "post_always"

        self.allCmds.append( cmd )
        return cmd


    def bulkInsertJobData (self):
        if isinstance(self.dbcursor, PlpyCursor):
            self.bulkInsertJobDataWithInsert()
        else:
            self.bulkInsertJobDataWithCopyFrom()

    def bulkInsertJobDataWithInsert (self):
        '''
        insert records using a series of insert statements
        '''

        try:
            insert = self.engineDB._insert(self.engineDB.JobTable, self.jobObj)
            self.dbcursor.execute(insert)
            for task in self.allTasks:
                insert = self.engineDB._insert(self.engineDB.TaskTable, task)
                self.dbcursor.execute(insert)
            for cmd in self.allCmds:
                insert = self.engineDB._insert(self.engineDB.CommandTable, cmd)
                self.dbcursor.execute(insert)
            self.dbcursor.execute("commit")
        except:
            # rollback?
            raise

    def bulkInsertJobDataWithCopyFrom(self):
        '''
        bulk insert records on a per-table basis using psql's COPY FROM
        '''

        try:
            self.dbcursor.execute("begin")

            s = StringIO.StringIO(PGFormat.formatObjsForCOPY([self.jobObj]))
            self.dbcursor.copy_from(s, "job")

            s = StringIO.StringIO(PGFormat.formatObjsForCOPY(self.allTasks))
            self.dbcursor.copy_from(s, "task")

            s = StringIO.StringIO(PGFormat.formatObjsForCOPY(self.allCmds))
            self.dbcursor.copy_from(s, "command")

            self.dbcursor.execute("end")

        except:
            # rollback?
            raise


    ## ---------------------------- ##
    def resolveTaskIntances (self):
        # called after all tasks have been instantiated,
        # (so alfred-style forward Instance references can be resolved)
        for t in self.allTasks:
            for ref in t.ants:
                # 'ref' is reference name given to Instance, indicating
                # a non-tree related predecessor (aka antecedent)
                try:
                    rtid = self.instanceMap[ref]
                    self.allTasks[rtid - 1].ptids.append( t.tid )
                except:
                    if self.dbgmode:
                        print >>sys.stderr, \
                            "Instance resolution failed T%d -> %s" % (t.tid, ref)

        # Now make another pass looking for cases where a single Instance
        # reference exists to an otherwise unconnected top-level task
        # (common for at least one studio's job generators).  In these
        # specific cases we can "convert" the Instance into a proper subtask
        # by reparenting the target, simplifying traversal and graph drawing.
        if not self.jobObj.serialsubtasks:
            for t in self.allTasks:
                if t.ptids[0]==0 and len(t.ptids)==2:
                    # only successor, other than job itself, is via Instance
                    # so convert the instanced task into a real subtask
                    ptid = t.ptids[1]
                    p = self.allTasks[ptid - 1]
                    p.kids.append( t.tid )
                    t.ptids = [ptid]


    ## ---------------------------- ##
    def getExpandOffsetsFromDB (self, ntasks, ncmds):
        #
        # See comment in bulkInsertExpandData regarding why we
        # need to atomically "reserve" a block of new tids/cids.
        # We SUBTRACT our known sizes from the returned MAX values
        # to give the new offsets.
        #
        self.dbcursor.execute(  "UPDATE job SET maxtid=maxtid+%d, " \
                                "maxcid=maxcid+%d WHERE jid=%s " \
                                "RETURNING maxtid,maxcid" % \
                                (ntasks, ncmds, self.jid) )
        rows = self.dbcursor.fetchall()
        row = rows[0]
        if isinstance(self.dbcursor, PlpyCursor):
            maxtid = row["maxtid"]
            maxcid = row["maxcid"]
        else:
            maxtid = row[0]
            maxcid = row[1]
        return (maxtid - ntasks, maxcid - ncmds)  # (tbase, cbase)


    ## ---------------------------- ##
    def adjustExpandIDs (self):

        # First, fix up the just-generated tid and cid values to be
        # non-colliding with tid/cid values in the existing job tables,
        # and other expands that may be running concurrently.  The json
        # conversion above started with tid=1,cid=1 and so now the
        # *index* tracking variables lastCID/lastTID can also be 
        # treated as max *counts* for this subtree parse.  We will
        # fetch the current job-wide max indexes from the db and treat
        # our just-generated tid/cid values as relative offsets from
        # those db max vals. We can't get the job maxs before doing
        # the processing above because we need to request the current
        # job's id maxs AND atomically increment them so that we have
        # a "reserved" block of ids for our own use. That is, we are
        # assuming that the db will enforce a serialized lock on that
        # fetch+incr so that concurrently running expand handlers
        # will each get a unique block of right-sized ids.

        ntasks = self.lastTID
        ncmds  = self.lastCID

        if ntasks != len(self.allTasks) or ncmds != len(self.allCmds):
            raise Exception("expand ntasks count anomaly " + \
                str(( ntasks, len(self.allTasks), ncmds, len(self.allCmds) )))

        if 0 == ntasks:
            return 0   # nothing else to do

        tidOffset, cidOffset = self.getExpandOffsetsFromDB( ntasks, ncmds)

        for c in self.allCmds:
            c.cid += cidOffset
            c.tid += tidOffset

        for t in self.allTasks:
            if t.tid < 0:
                t.tid = -t.tid
            else:
                t.tid  += tidOffset
            t.ptids = [p+tidOffset if p > 0 else -p for p in t.ptids]
            t.cids  = [c+cidOffset for c in t.cids]

        self.tidOffset = tidOffset

        # FIXME - fix antecedent references? can't incr the ones that point
        # to tasks outside this expand, but adjust those "local" to expand

        return ncmds

    ## ---------------------------- ##

    def bulkInsertExpandData (self):
        if isinstance(self.dbcursor, PlpyCursor):
            self.bulkInsertExpandDataWithInsert()
        else:
            self.bulkInsertExpandDataWithCopyFrom()


    def bulkInsertExpandDataWithInsert (self):

        # Now insert the new lists -- assumes we are running as plpy "in" the db
        try:
            for task in self.allTasks:
                insert = self.engineDB._insert(self.engineDB.TaskTable, task)
                self.dbcursor.execute(insert)
            for cmd in self.allCmds:
                insert = self.engineDB._insert(self.engineDB.CommandTable, cmd)
                self.dbcursor.execute(insert)

            # Now adjust the job's tasks count caches
            self.dbcursor.execute( "UPDATE job SET " \
                                    "numtasks=numtasks+%d, " \
                                    "numready=numready+%d, " \
                                    "numblocked=numblocked+%d " \
                                    "WHERE jid=%s" % \
                                    (len(self.allTasks), self.nready,
                                     len(self.allTasks) - self.nready , self.jid)
                                 )
        except:
            # rollback?
            raise


    def bulkInsertExpandDataWithCopyFrom (self):

        # Now insert the new lists -- assumes we are a db client
        try:
            self.dbcursor.execute("begin")

            ntasks = len(self.allTasks)
            if ntasks > 0:
              s = StringIO.StringIO( PGFormat.formatObjsForCOPY(self.allTasks) )
              self.dbcursor.copy_from(s, "task")

            ncmds = len(self.allCmds)
            if ncmds > 0:
              s = StringIO.StringIO( PGFormat.formatObjsForCOPY(self.allCmds) )
              self.dbcursor.copy_from(s, "command")

            # Now adjust the job's tasks count caches
            self.dbcursor.execute( "UPDATE job SET " \
                                    "numtasks=numtasks+%d, " \
                                    "numready=numready+%d, " \
                                    "numblocked=numblocked+%d " \
                                    "WHERE jid=%s" % \
                                    (ntasks, self.nready,
                                     ntasks-self.nready, self.jid)
                                 )

            self.dbcursor.execute("end")
        except:
            # rollback?
            raise


    ## ---------------------------- ##
    def tWalk (self, tindex):
        task = self.allTasks[tindex]
        return {
            "data": {
                "tid":   task.tid,
                "title": task.title,
                "id":    task.id,
                "cids":  task.cids,
                "ptids": task.ptids
            },
            "children": [self.tWalk(k) for k in task.kids]
        }

    def cxFmt (self, cmd):
        ctp = "L" if cmd.local else "R"

        rtd = { "cleanup":      "D",
                "post_always":  "P",
                "post_error":   "PE",
                "post_done":    "PD" }
        try:
            ctp += rdt[cmd.runtype]
        except:
            ctp += "C"

        ctp += "X" if cmd.expand else ""

        return {
            "cid":      cmd.cid,
            "argv":     cmd.argv,
            "msg":      cmd.msg,
            "type":     ctp,
            "service":  cmd.service,
            "tags":     cmd.tags,
            "id":       cmd.id,
            "refersto": cmd.refersto,
            "minSlots": cmd.minslots,
            "maxSlots": cmd.maxslots,
            "envkey":   cmd.envkey,
            "retryrc":  cmd.retryrcodes
        }

## ---------------------------- ##
        
if __name__ == "__main__":
    main()
