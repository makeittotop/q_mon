#
TrFileRevisionDate = "$DateTime: 2014/09/08 12:33:42 $"

#
# tractor-spool - Spool a new job into the Tractor job queue.
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2014 Pixar Animation Studios. All rights reserved.
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

import os
import sys
import platform
import datetime
import optparse
import getpass
import socket
import urllib
import time
import re
import json

## --------- ##
# version check
if sys.version_info < (2, 7):
    print >>sys.stderr,"Error: tractor-spool requires python 2.7 (or later)\n"
    sys.exit(27)
## --------- ##

from tractor.base.TrHttpRPC import TrHttpRPC
import _alfparse

s_spoolingProtocolVersion = "2.0"

## --------- ##

def main (argv):
    appName =        "tractor-spool"
    appVersion =     "TRACTOR_VERSION"
    appProductDate = "TRACTOR_BUILD_DATE"

    if not appProductDate[0].isdigit():
        appProductDate = " ".join(TrFileRevisionDate.split()[1:3])
        appVersion = "dev"

    appVersDate = "%s %s (%s)\nCopyright (c) 2007-%d Pixar. " \
                  "All rights reserved." \
                   % (appName, appVersion, appProductDate,
                        datetime.datetime.now().year)

    rc, xcpt, options = parseOptions( argv, appName, appVersDate, None )

    # rc may be zero and options empty if the
    # optparser itself declared exit(0), like for --version
    if rc==0 and not options:
        return 0

    if rc != 0 and not xcpt:
        xcpt = "error parsing spooler args"
    if xcpt:
        print >>sys.stderr, xcpt
        return rc

    if options.loglevel > 1:
        print >>sys.stderr, appVersDate

    if options.jdel_id:
        if len(options.jobfiles) == 0:
            return handleJobDelete(options)
        else:
            xcpt = "too many arguments for jdelete"
            return 1
    else:
        return processJobFiles( options )
        

## ----------------------- ##
def parseOptions (argv, progname, appVersionInfo, altUsage):
    '''
    examine tractor job spooling options
    '''

    usageStr =  "Send jobs to the tractor job queue:\n" \
                "%prog [options] JOBFILE...\n" \
                "%prog [options] -c appname appArg1 appArg2 ...\n" \
                "%prog [options] --ribs frm1.rib frm2.rib ...\n" \
                "%prog [options] --rib frm1_prologue.rib frm1.rib...\n" \
                "%prog [options] --jdelete=JOB_ID --user=JOB_OWNER\n"

    defaultMtd  = "tractor-engine:80"

    spoolhost = socket.gethostname().split('.')[0] # options can override
    processOwner = getpass.getuser()

    # ------ #
    if altUsage:
        usageStr = altUsage

    optparser = optparse.OptionParser( prog=progname,
                                       version=appVersionInfo,
                                       usage=usageStr )

    try:
        # sometimes current dir has been deleted out from under caller
        curdir = trAbsPath(os.getcwd())
    except:
        curdir = "."

    optparser.disable_interspersed_args()

    optparser.set_defaults(jobfiles=[])
    optparser.set_defaults(altuser=0)

    optparser.set_defaults(loglevel=1)
    optparser.add_option("-v", "--debug",
            action="store_const", const=2, dest="loglevel",
            help="verbose status")
    optparser.add_option("-q",
            action="store_const", const=0, dest="loglevel",
            help="quiet, no status")

    optparser.add_option("--engine", dest="mtdhost",
            type="string", default=defaultMtd,
            help="hostname[:port] of the master tractor daemon, "
                 "default is '"+defaultMtd+"' - usually a DNS alias")

    optparser.add_option("--hname", dest="hname",
            type="string", default=spoolhost,
            help="the origin hostname for this job, used to find the "
                 "'home blade' that will run 'local' Cmds; default is "
                 "the locally-derived hostname")
    optparser.add_option("--haddr", dest="haddr",
            type="string", default="",
            help="address of remote spooling client")

    optparser.add_option("--jobcwd", dest="jobcwd",
            type="string", default=curdir,
            help="blades will attempt to chdir to the specified directory "
                 "when launching commands from this job; default is simply "
                 "the cwd at time when tractor-spool is run")

    optparser.set_defaults(inboundFormat="ALFRED")
    optparser.add_option("--in-json", "-J", dest="inboundFormat",
            action="store_const", const="JSON",
            help="specified job file is formatted as Tractor compliant JSON")
    optparser.add_option("--in-alfred", "-A", dest="inboundFormat",
            action="store_const", const="ALFRED",
            help="specified job file is in Alfred (tcl) format, the default")

    optparser.add_option("--priority", dest="priority",
            type="string", default="default",
            help="priority of the new job (float)")

    optparser.add_option("--projects", dest="projects",
            type="string", default="default",
            help="list of project affiliations, like 'TheFilm lighting'")

    optparser.add_option("--tier", dest="tier",
            type="string", default=None,
            help="dispatching tier assignment, for special-case jobs")

    optparser.add_option("--paused", dest="paused",
            action="store_true", default=False,
            help="submit job in paused mode")

    optparser.add_option("--svckey", "--service", dest="svckey",
            type="string", default="",
            help="specifies an additional job-wide service key restriction "
                 "for Cmds in the spooled job, the key(s) are ANDed with any "
                 "keys found on the Cmds themselves.  When used with -c "
                 "or --rib option, it specifies the sole service key used to "
                 "select matching blades for those Cmds, default: pixarRender")

    optparser.add_option("--cmdsvckey", "--cmdservice", dest="cmdsvckey",
            type="string", default="",
            help="used with -c or --rib option to specify the sole service "
                 "key expression for each command; subject to substitution "
                 "by RANGE and ITER")

    optparser.add_option("--remotecleankey", dest="remoteclean",
            metavar="SVCKEY", type="string", default="",
            help="convert local Cmds in job clean-up sections to RemoteCmds "
                 "that target the given blade type.  This conversion is also "
                 "applied to Job -whenerror and -whendone blocks in older "
                 "scripts that do not specify the command type explicitly "
                 "in a Job -postscript block.  This conversion option is a "
                 "workaround for cases where the job script generator "
                 "itself cannot be updated to use RemoteCmd directly.")

    optparser.add_option("--envkey", dest="envkey",
            type="string", default=None,
            help="used with -c and --rib to change the environment key used "
                 "to configure the environment variables on the blades, "
                 "default: None")

    optparser.add_option("--title", dest="jtitle",
            type="string", default=None,
            help="optional title for the auto-generated job, "
                 "when using -c or -r")

    optparser.add_option("--aftertime", dest="aftertime",
            type="string", default=None,
            help="delay job start until the given date, as 'MM DD HH:MM'")

    optparser.add_option("--afterjid", dest="afterjid",
            type="string", default=None,
            help="delay job start until the given job(s) complete, "
                 "specified as jid(s)")

    optparser.add_option("--maxactive", dest="maxactive",
            type="int", default=None,
            help="limit the maximum number of concurrently active commands of job")

    optparser.set_defaults(cmdmode="RemoteCmd")
    optparser.set_defaults(argmode="jobfiles")
    optparser.set_defaults(autocmdargs=[])
    
    optparser.add_option("--command", "-c", "-C",
                action="callback", callback=collectAutoCmdArgs,
                help="create a single-task job to execute the given command "
                     "on an available remote tractor-blade server; "
                     "all remaining arguments are collected and passed to "
                     "the named program. Use -c for the usual RemoteCmd "
                     "format, and -C to force a local Cmd.")

    optparser.add_option("--range", dest="range",
            type="string",
            help="create one task for each integer in the range, "
                 "substituting RANGE in the command string with the "
                 "current range number; e.g. '1-10' or '1,4,9,16-20'")

    optparser.add_option("--itervalues", dest="itervalues",
            type="string",
            help="create one task for each item in the list, "
                 "specified by a comma-separated list of items, "
                 "replacing ITER in the command string or serivce key "
                 "expression with the current value; "
                 "e.g. red,green,blue or 'red house,green lawn,blue sky'")

    optparser.add_option("--iterfile", dest="iterfile",
            type="string",
            help="create one task for each item in the file, "
                 "which specifies a line-separated list of items ")

    optparser.add_option("--ribs", "-r", dest="argmode",
            action="store_const", const="ribmulti",
            help="treat the jobfile filename arguments as individual RIB files "
                 "to be rendered as INDEPENDENT prman processes on different "
                 "remote tractor-blades; a multi-task Tractor job is "
                 "automatically created to handle the renderings")

    optparser.add_option("--rib", "-R", dest="argmode",
            action="store_const", const="ribsingle",
            help="treat the jobfile filename arguments as RIB files to be "
                 "rendered using a SINGLE prman process on a remote "
                 "tractor-blade; a single-task Tractor job is automatically "
                 "created to handle the rendering")

    optparser.add_option("--nrm", dest="nrm",
            action="store_true", default=False,
            help="causes auto-generated --rib jobs to use netrender on "
                 "the local blade rather than direct rendering with prman "
                 "on a blade; used when the named RIBfile is not accessible "
                 "from the remote blades directly")

    optparser.add_option("--spool-wait", dest="spoolWait",
            action="store_true", default=False,
            help="block until job is fully spooled (to tractor-engine 2.0+)")

    optparser.add_option("--alfescape", dest="alfescape",
            action="store_true", default=False,
            help="use alfred-compatible two-level unquoting")

    optparser.set_defaults(emitJSON=False)
    optparser.add_option("--status-json", dest="emitJSON", action="store_true",
            help="prints spool confirmation message (or denial) "
                 "as a JSON-format dict on stdout")
    optparser.add_option("--status-plain", dest="emitJSON", action="store_false",
            help="prints spool confirmation message as human-readable "
                 "plain text; this is the default")

    optparser.add_option("--user", dest="jobOwner",
            type="string", default=processOwner,
            help="alternate job owner, default is user spooling the job")
    optparser.add_option("-p", "--passwd", dest="passwd",
            type="string", default=None,
            help="login password for job owner (if engine passwords enabled)")
    optparser.add_option("--configfile", dest="configfile",
            type="string", default=None,
            help="file containing login and password data")

    optparser.add_option("--review", "--print-alfscript", "-o", dest="alfout",
            action="store_true", default=False,
            help="print the job alfscript rather than spooling it; usually "
                 "to view or save the job text that tractor-spool itself "
                 "has generated based on other arguments, rather than when "
                 "it is reading an existing job from a file.")
    optparser.add_option("--review-debug", "--print-jobjson", "-O",
            dest="dbgjson", action="store_true", default=False,
            help="print inbound and outbound job text to stdout, "
                 "for debugging purposes, to stdout rather than spooling it")

    optparser.add_option("--jdelete", "--jretire", dest="jdel_id",
            type="string", default=None,
            help="delete the requested job from the active queue")

    rc = -1
    xcpt = "error parsing options"  # generic, changed below
    options = None

    try:
        options, trailing = optparser.parse_args(argv)
        
        if options.argmode == "autocmd":
            options.autocmdargs = trailing
            options.jobfiles = []
        else:
            options.jobfiles = trailing

        if 0 == len(options.jobfiles) and options.argmode != "autocmd":
            raise Exception("no job script file(s) specified")

        elif options.range and options.itervalues or \
             options.range and options.iterfile or \
             options.itervalues and options.iterfile:
                xcpt = "only one of --range, --iterfile, and " \
                       "--itervalues can be specified at once"

        if processOwner != options.jobOwner:
            options.altuser = 1

        #
        # allow $TRACTOR_ENGINE to set the engine hostnames, if it exists 
        # in the environment, UNLESS it has been explicitly set on the
        # command line
        #
        if options.mtdhost == defaultMtd:
            options.mtdhost = os.getenv('TRACTOR_ENGINE', defaultMtd)

        if options.mtdhost != defaultMtd:
            h,n,p = options.mtdhost.partition(":")
            if not p:
                options.mtdhost = h + ':80'

        # apply --rib handler by default if all files end in ".rib"
        if options.argmode == "jobfiles" and \
            reduce(lambda x, y: x and y,
                    [f.endswith('.rib') for f in options.jobfiles]):
            options.argmode = 'ribmulti'

        if options.alfout or options.dbgjson:
            options.loglevel = 0

        # successful option parsing!
        rc = 0
        xcpt = None

    except SystemExit, e:
        options = None
        rc = e.code
        if rc:
            xcpt = "exit(%d)" % rc
        else:
            xcpt = None

    except:
        errclass, excobj = sys.exc_info()[:2]
        xcpt = "tractor-spool option parsing error: %s - %s" % (errclass.__name__, str(excobj))
        rc = -1

    return (rc, xcpt, options)


## ------------------------------------------------------------- ##
def parseAndDeliver (options, jobfilename, alftxt):

    if options.alfout or options.dbgjson:
        if options.dbgjson:
            print "\n---- alfscript ----\n"
        if alftxt:
            print alftxt
        else:
            f = open(jobfilename, "r");  print f.read();  f.close()

        if options.alfout:
            sys.exit(0)

    jobjson = _alfparse.ParseAlfJob( options, jobfilename, alftxt )

    if 0 == len(jobjson) or '{' != jobjson[0]:  # will close }
        return (38, "alfscript parse failed to produce JSON")

    if options.dbgjson:
        print "---- json ----\n\n", jobjson
        return (0, "")

    try:
        syntaxChkOnly = json.loads( jobjson )
    except:
        errclass, excobj = sys.exc_info()[:2]
        return (39, "Invalid JSON generated from job script parse,\n"
                    "   try --alfescape, or inspect with --review-debug.\n"
                    "   python json module error from the GENERATED JSON:\n"
                    "   %s - %s" % (errclass.__name__, str(excobj)) )

    return deliverToEngine(jobfilename, jobjson, alftxt, options)


## ------------------------------------------------------------- ##
def processJobFiles (options):
    '''
    spool new jobs!
    '''
    try:
        if options.argmode == "jobfiles":

            for fname in options.jobfiles:

                if not os.access( fname, os.R_OK ):
                    rc = 1
                    xcpt = "tractor-spool - no access to file: " + fname

                elif options.inboundFormat.upper() == "JSON":
                    f = open(fname, "r")
                    jobjson = f.read()
                    f.close()
                    if options.dbgjson:
                        print "---- json ----\n\n", jobjson
                        rc, xcpt = (0, "")
                    else:
                        syntaxChkOnly = json.loads( jobjson )
                        rc, xcpt = deliverToEngine(fname, jobjson, None, options)

                else:
                    rc, xcpt = parseAndDeliver( options, fname, None )

                if rc:
                    break
        else:
            rc, jtxt = createAutoJob(options.jobfiles, options)
            if 0 != rc:
                xcpt = jtxt
            elif rc == 0:
                if 0 == len(options.jobfiles):
                    labelname = "tractor-spool-generated"
                else:
                    labelname = options.jobfiles[0]

                rc, xcpt = parseAndDeliver( options, labelname, jtxt )

        # "xcpt" usually contains the success JSON msg.
        # A 404 rc usually means that we connected to a regular web server
        # rather than to a tractor-engine.
        if rc == 404:
            xcpt = "could not contact tractor spooler on " + \
                    options.mtdhost + " -- " + xcpt

    except KeyboardInterrupt:
        xcpt = "received keyboard interrupt"
        rc = -1

    except SystemExit, e:
        rc = e.code
        xcpt = "{ \"rc\": " + str(e) + ", \"msg\": \"exit\" }"

    except:
        errclass, excobj = sys.exc_info()[:2]
        xcpt = "job spool: %s - %s" % (errclass.__name__, str(excobj))
        rc = -1

    if rc and xcpt:
        if options.emitJSON:
            xcpt = json.dumps( {"rc": rc, "msg": xcpt} )
        elif options.loglevel > 0:
            # emit an error diagnostic to stderr
            print >>sys.stderr, xcpt

    # emit a json msg to stdout
    if xcpt and options.emitJSON:
        print xcpt

    elif rc==0 and xcpt and options.loglevel > 0:
        # non-json success msg requested
        try:
            x = eval(xcpt.strip())
            xcpt = "OK " + x['msg']
        except:
            pass
        print xcpt

    return rc

## ------------------------------------------------------------- ##
def deliverToEngine (jfname, jobjson, jobalf, options):
    #
    # we are a client spooling a job from a user host
    # so deliver the json to the engine
    #
    try:
        connector, tsid = tractorLogin(options)
    except Exception, e:
        return (11, str(e))

    # we assume that we are talking to a tractor-engine vers 2.0+
    # but try again with 1.x syntax if spool request is rejected with rc=501

    rq = "jspool?q=newjob&spvers=" + s_spoolingProtocolVersion

    rr  = "&hnm=" + urllib.quote(options.hname)
    rr += "&jobFile=" + urllib.quote(jfname)
    rr += "&jobOwner=" + urllib.quote(options.jobOwner)
    rr += "&format=JSON"
    if tsid:
        rr += "&tsid=" + tsid

    if options.spoolWait:
        rr += "&blocking=spool"

    hdrs = {"Content-Type": "application/tractor-spool-json"}

    rc,d = connector.Transaction(rq + rr, jobjson, None, hdrs)
    
    if rc == 501:
        # retry with Tractor 1.x semantics, using alfscript variant
        rq = "spool?q=newjob"
        if not jobalf:
            f = open(jfname, "r");  jobalf = f.read();  f.close()

        hdrs = {"Content-Type": "application/tractor-spool"}

        rc,d = connector.Transaction(rq + rr, jobalf, None, hdrs)

    return (rc,d)


## ------------------------------------------------------------- ##
def collectAutoCmdArgs (option, opt_str, value, parser):
    if opt_str == "-C":
        parser.values.cmdmode = "Cmd" # local Cmd

    parser.values.argmode = "autocmd"

## ------------------------------------------------------------- ##
def trAbsPath (path):
    '''
    Generate a canonical path for tractor.  This is an absolute path
    with backslashes flipped forward.  Backslashes have been known to
    cause problems as they flow through the system, especially in the 
    Safari javascript interpreter.
    '''
    return os.path.abspath(path).replace('\\', '/')

## ------------------------------------------------------------- ##
_range2listRE = re.compile(r"([\-]?\d+)(?:\-([\-]?\d+))?")

def range2list(str, step=1):
    """
    This routine takes a range string and returns a list of numbers.
    e.g. '1-3,5-6' -> [1,2,3,5,6]

    >>> range2list('1-5')
    [1, 2, 3, 4, 5]

    >>> range2list('1-3,5-6')
    [1, 2, 3, 5, 6]

    >>> range2list('1,3,5,7,9')
    [1, 3, 5, 7, 9]
    
    >>> range2list('-14')
    [-14]
    
    >>> range2list('-14--10')
    [-14, -13, -12, -11, -10]
    
    """

    l = []
    ranges = str.split(',')

    for r in ranges:
        if r == '': continue

        # check for a match
        match = _range2listRE.match(r)
        if not match:
            raise StringUtilError, "range2list: expected an integer, " \
                  "got '%s'" % r

        first,last = match.groups()
        if not last:
            # this occurs if there was no '-'
            l.append(int(first))
        else:
            for i in range(int(first), int(last) + 1):
                l.append(i)

    if step > 1:
        lstep = []
        for i in range(0, len(l), step):
            lstep.append(l[i])
        l = lstep
                
    return l

## ------------------------------------------------------------- ##
def file2lines(filename):
    """Read the given file and return a list of strings, each string
    one line in the file."""
    f = open(filename)
    content = f.read()
    f.close()
    rawlines = content.split("\n")
    lines = []
    for rawline in rawlines:
        line = rawline.strip()
        if line:
            lines.append(line)
    return lines


## ------------------------------------------------------------- ##
def createRibTask (ribfiles, options):

    single = type(ribfiles) in (str, unicode)

    jtxt = "  Task -title {"
    if single:
        jtxt += ribfiles
    else:
        jtxt += " ".join([os.path.basename(f) for f in ribfiles])

    jtxt += "} -cmds {\n"

    if options.nrm:
        jtxt += "    Cmd {netrender %H -f -Progress"
    else:
        jtxt += "    RemoteCmd {prman -Progress"

    if single:
        jtxt += ' "' + ribfiles + '"'
    else:
        jtxt += ' "' + '" "'.join(ribfiles) + '"'

    jtxt += '} -tags {prman}'

    if not options.svckey:
        # if a job-wide key has not already been added to the request ...
        jtxt += ' -service {pixarRender}'

    jtxt += "\n  }\n"  # end of cmds
    return jtxt
    
## ------------------------------------------------------------- ##
def createArbTask (cmdargs, options, rangeValue=None, iterValue=None):
    title = os.path.basename(cmdargs[0])
    argv = cmdargs[:]
    cmdstring = "{" + "} {".join(argv) + "}"
    if rangeValue:
        cmdstring = cmdstring.replace("RANGE", str(rangeValue))
    if iterValue:
        cmdstring = cmdstring.replace("ITER", str(iterValue))
    svckey = ""
    if options.cmdmode == "RemoteCmd":
        if options.cmdsvckey:
            svckey = " -service {%s}" % options.cmdsvckey
            svckey = svckey.replace("RANGE", str(rangeValue))
            svckey = svckey.replace("ITER", str(iterValue))
        elif not options.svckey: 
            # need a task-level service key if no job-level service key is defined
            svckey = " -service {pixarRender}"
    
    text = "  Task -title {%s} -cmds {\n" \
           "    %s {%s}%s\n" \
           "  }\n" % (title, options.cmdmode, cmdstring, svckey)

    return text
    
## ------------------------------------------------------------- ##
def createAutoJob (ribfiles, options):

    if options.argmode == "autocmd":
        cmdargs = options.autocmdargs
        if (list != type(cmdargs) or \
            0 == len(cmdargs) or \
            0 == len(cmdargs[0])):
            return (17, "invalid empty command")

        if cmdargs[0][0] == '=':
            # look for a built-in tractor "equivalence" (like an alias)
            # so "tractor-spool -c =printenv" becomes
            # RemoteCmd {TractorBuiltIn printenv}
            # tractor-blade recognizes several of these handy "portability"
            # verbs like: "sleep", "file", "echo", "printenv", "system"
            op = cmdargs[0][1:]
            cmdargs[0] = op
            cmdargs.insert(0, "TractorBuiltIn")
            if not options.jtitle:
                options.jtitle = op

    jtxt = "##AlfredToDo 3.0\n"
    jtxt += "Job -title {"
    if options.jtitle:
        jtxt += options.jtitle
    elif options.argmode == "autocmd":
        jtxt += cmdargs[0]
    else:
        jtxt += os.path.basename(ribfiles[0])
        if len(ribfiles) > 1:
            jtxt += " ..."

    jtxt += "} "

    if options.tier:
        jtxt += "-tier {%s} " % options.tier
    
    if options.projects:
        jtxt += "-projects {%s} " % options.projects
    
    jtxt += "-subtasks {\n"

    if options.argmode == "ribmulti":
        for f in ribfiles:
            jtxt += createRibTask(f, options)

    elif options.argmode == "ribsingle":
        jtxt += createRibTask(ribfiles, options)

    elif options.argmode == "autocmd":
        if options.range:
            vals = range2list(options.range)
            for val in vals:
                jtxt += createArbTask(cmdargs, options, rangeValue=val)
        elif options.itervalues:
            vals = options.itervalues.split(",")
            for val in vals:
                jtxt += createArbTask(cmdargs, options, iterValue=val)
        elif options.iterfile:
            vals = file2lines(options.iterfile)
            for val in vals:
                jtxt += createArbTask(cmdargs, options, iterValue=val)
        else:
            jtxt += createArbTask(cmdargs, options)

    jtxt += "}\n"    # end of job

    return (0, jtxt)

## ------------------------------------------------------------- ##
def tractorLogin (options):        
    client = options.jobOwner
    password = None
    host,x,port = options.mtdhost.partition(':')

    if options.configfile:
        # try to parse the file, or throw exception on failure
        try:
            f=open(options.configfile, "r")
        except:
            print >>sys.stderr,"unable to locate config file: %s" % options.configfile
            raise
        try:
            data=f.read()
            f.close()
            # file is supposed to contain user and password at a minumum,
            # raises an exception if not defined
            configdict = eval(data)
            tractordict =  configdict["tractorconfig"]
            client = tractordict["user"]
            password = tractordict["passwd"]
            
            # it may also contain optional config information like
            # engine hostname and port (called "monitor"), don't raise 
            # an exception if they do not exist.
            if "monitor" in tractordict:
                host,x,port = tractordict["monitor"].partition(':')
            if "port" in tractordict:
                port = tractordict["port"]
        except:
            raise Exception("invalid config file: "+options.configfile)

    lmthdr = {
        'User-Agent': "tractor-spool",
        'Host': "%s:%s" % (host, port)
    }
    connector = TrHttpRPC(host, int(port), apphdrs=lmthdr, timeout=3600)

    #
    # Current policy is skip password checks on job spools unless
    # the logged in (client) user has chosen to spoof the user name
    # attached to the job.
    #
    if not options.altuser:
        return (connector, None)

    #
    # check whether passwords are even enabled on the engine
    #
    passwordRequired = connector.PasswordRequired()
    if not passwordRequired:
        return (connector, None)

    if not password:
        if options.passwd:  password = options.passwd
        else: password = getpass.getpass("Enter password for %s: " % client)

    try:
        data =  connector.Login(client, password)
        tsid = data['tsid']     # already validated by the connector

    except Exception, e:
        errclass, excobj = sys.exc_info()[:2]
        print >>sys.stderr,"%s - %s" % (errclass.__name__, str(excobj))      
        raise RuntimeError()

    if options.loglevel > 1:
        print >>sys.stderr,"successful login for %s:%s" %(client, str(tsid))

    return (connector, tsid)

## ------------------------------------------------------------- ##
def handleJobDelete (options):
    '''
    Request that a job be deleted from the tractor queue
    '''
    # login first to allow job delete
    try:
        (connector, tsid) = tractorLogin(options)
    except:
        return
    
    sjid = str(options.jdel_id)

    q =  "q=jretire&jid=" + sjid
    q += "&hnm=" + options.hname
    q += "&tsid=%s&login=%s" % (tsid, options.jobOwner)

    rc, msg = connector.Transaction("queue", q)

    if 0 == rc:
        print "J" + sjid + " delete OK"
    else:
        print msg

    return rc

## ------------------------------------------------------------- ##
## ------------------------------------------------------------- ##

if __name__ == "__main__":
    rc = main( sys.argv[1:] )
    if 0 != rc:
        sys.exit(rc)
