#
# TrSysState - obtain system information and performance values
#
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

import os, sys, platform, time, socket, errno
import logging, subprocess, math, fnmatch
import ctypes, ctypes.util

if sys.platform == 'win32':
    from ctypes.wintypes import HWND, HANDLE, DWORD, LPCWSTR, MAX_PATH

elif sys.platform == 'darwin':
    tr_ctypes_libc = ctypes.CDLL( ctypes.util.find_library("c") )

## --------------------------------------------------------- ##

class TrSysState(object):
    """
    The TrSysState object manages access to certain static and dynamic
    system characteristics of the blade host.  Its goal is to provide
    a bit of an abstraction for accessing these values across different
    operating systems.
    """

    def __init__(self, opts, addr):
        self.logger = logging.getLogger("tractor-blade")

        self.hostClkTck = 100
        self.nCPUs = self.countCPUs()
        self.physRAM = self.getPhysicalRAM()
        self.boottime = self.getBootTime()

        self.processOwner = opts.processOwner
        self.hostname = "localhost"
        self.aliases = ["localhost"]
        self.addrs = ["127.0.0.1"]
        self.altname = opts.altname
        self.resolveHostname(addr)

        self.resolveOS() # sets self.osType, self.osInfo

        self.chkptfile = self.resolveCheckpointFilename( opts )

        self.cpuOldIdle = None
        self.cpuOldKernel = None
        self.cpuOldUser = None
        self.GetProcessorUsage()  # prime with initial snapshot


    def resolveOS (self):
        if sys.platform == 'win32':
            self.osType = "Windows"
            p = platform.platform() + "-" + platform.architecture()[0]
            if fnmatch.fnmatch(p, "Windows-*-6.3*-64*"):
                w = "8.1"
            elif fnmatch.fnmatch(p, "Windows-*-6.2*-64*"):
                w = "8"
            elif fnmatch.fnmatch(p, "Windows-*-6.1*-64*"):
                w = "7"
            elif fnmatch.fnmatch(p, "Windows-*-6.1*-32*"):
                w = "7"
            elif fnmatch.fnmatch(p, "Windows-*-6.0*-64*"):
                w = "Vista"
            elif fnmatch.fnmatch(p, "Windows-*-5.*"):
                w = "XP"
            else:
                w = "XP"
            self.osInfo = "Windows, " + w + ", " + p
    
        elif sys.platform == 'darwin':
            self.osType = "MacOS"
            self.osInfo = "MacOSX, " + \
                          platform.mac_ver()[0] + ", " + \
                          platform.platform()
        else:
            self.osType = "Linux"
            d = platform.linux_distribution()
            self.osInfo = d[0]+", "+d[1]+", "+ platform.platform()


    def GetProcessorUsage (self):
        if sys.platform == 'win32':
            return self.getProcessorUsage_Windows()
        else:
            # cpu load, normalized by number of CPUs (if reqd)
            # so a fully loaded machine is always "1.0" independent
            # of the number of processors.
            return( os.getloadavg()[0] / float(self.nCPUs) )


    def countCPUs(self):
        '''
        Returns the number of CPUs on the system
        '''
        num = 0
        try:
            if sys.platform == 'win32':
                # here are the usual win32 C calls, worth doing this via ctypes?
                #    SYSTEM_INFO sysinfo;
                #    GetSystemInfo(&sysinfo);
                #    num = sysinfo.dwNumberOfProcessors;
                #
                num = int(os.environ['NUMBER_OF_PROCESSORS'])

            elif sys.platform == 'darwin':
                ncpu = ctypes.c_uint(0)
                size = ctypes.c_size_t( ctypes.sizeof(ncpu) )
                try:
                    rc = tr_ctypes_libc.sysctlbyname("hw.ncpu",
                                ctypes.byref(ncpu), ctypes.byref(size),
                                None, 0)
                except:
                    rc = -1

                if 0 == rc:
                    num = ncpu.value
                else:
                    # on failure, revert to popen and execing sysctl
                    num = int(os.popen('sysctl -n hw.ncpu').read())

            else:
                # GNU/Linux
                num = os.sysconf('SC_NPROCESSORS_ONLN')
                self.hostClkTck = os.sysconf('SC_CLK_TCK')

        except Exception:
            # we can pretty much assume that since we're running,
            # there must be at least one running cpu here somewhere
            num = 1

        if num < 1:
            num = 1
            
        return int( num )


    def resolveHostname (self, reflectedAddr=None):
        lnm = "localhost";
        try:
            lnm = socket.gethostname()
            hostname,aliases,addrs = socket.gethostbyname_ex(lnm)
            aliases.append(hostname)
            hnm = lnm.split('.')[0] # matches tractor-spool.py

            # As a special case for sites with weird DNS issues, the
            # blade cmdline option --hname=. (a dot rather than a name)
            # forces the "altname" to be the name found with the above
            # call, independent of what the reverse addr lookup finds.
            if '.' == self.altname:
                self.altname = hnm

        except Exception, e:
            self.logger.error("cannot resolve network hostname info: " + \
                                lnm + " " + str(e))
            hnm = lnm
            aliases = []
            addrs = ['127.0.0.1']

            aliases.append(hnm)

            # also add alias with stripped osx off-the-grid suffix
            if hnm.endswith(".local"):
                hnm = hnm[:-6] # strip .local
                aliases.append(hnm)

            try:
                h = hnm.split('.')[0]
                if h != hnm:
                    h2,al2,ad2 = socket.gethostbyname_ex(h)
                    if h2 == hnm:
                        hnm = h
                        aliases.append(h)
                        aliases.extend(al2)
                        addrs.extend(ad2)
            except Exception:
                pass

        if reflectedAddr:
            # prefer the identity associated with our route to the engine
            # since blades.config host match criteria is defined to be
            # the "as seen by the engine" names.  So always force the
            # engine's view into the addr list.
            try:
                addrs += [reflectedAddr] # add addr as seen by engine

                # and also try to resolve that addr's name via the nameserver
                h2,al2,ad2 = socket.gethostbyaddr( reflectedAddr )
                aliases.append(h2)
                aliases.extend(al2)
                addrs.extend(ad2)
                hnm = h2.split('.')[0]
            except:
                pass

        if self.altname and self.altname != hnm:
            # When --hname=some_name is given on the cmdline, then we
            # use that name execlusively for profile matching by NAME.
            # Otherwise the blade might match a profile designed
            # specifically for the usual hostname.  However we do
            # also look up the given hname to see if there are
            # aliases defined specifically for it.
            #
            hnm = self.altname
            aliases = []
            try:
                h2,al2,ad2 = socket.gethostbyname_ex(hnm)
                aliases.append(h2)
                aliases.extend(al2)
                addrs.extend(ad2)
            except Exception:
                pass

        self.hostname = hnm

        if len(addrs) > 1:
            addrs = self.uniquifySequence(addrs)
            # remove 127.0.0.1 if there are other choices
            try:
                if len(addrs) > 1 and reflectedAddr != '127.0.0.1':
                    addrs.remove('127.0.0.1')
            except Exception:
                pass
        self.addrs = addrs

        if len(aliases) > 1:
            aliases = self.uniquifySequence(aliases)

        # also remove extraneous reverse-lookup x.y.z.inaddr-any.arpa
        # entries created by python socket.gethostbyaddr() on some platforms
        self.aliases = []
        for a in aliases:
            if not a.endswith(".arpa"):
                self.aliases.append(a)

        self.logger.info("resolved local hostname: '%s'  %s" % \
                (self.hostname, ",".join(self.addrs)))
        if len(self.aliases) > 0:
            self.logger.debug("(aliases: " + 
                    ", ".join(self.aliases) + ')')


    def getBootTime (self):
        ## FIXME: for python3 we can just use this on all platforms:
        ##   tstamp = uptime.boottime()

        tstamp = time.time()
        try:
            if sys.platform == 'win32':
                GetTickCount64 = ctypes.windll.kernel32.GetTickCount64
                GetTickCount64.restype = ctypes.c_ulonglong
                tstamp = time.time() - (GetTickCount64() / 1000.0)

            elif sys.platform == 'darwin':
                try:
                    class TIMEVAL(ctypes.Structure):
                        _fields_ = [("tv_sec", ctypes.c_long),
                                    ("tv_usec", ctypes.c_int32)]
                    tv = TIMEVAL()
                    tvsz = ctypes.c_size_t( ctypes.sizeof(tv) )
                    rc = tr_ctypes_libc.sysctlbyname("kern.boottime",
                                ctypes.byref(tv), ctypes.byref(tvsz), None, 0)

                    if rc == 0:
                        tstamp = (tv.tv_usec / 1000000.0) + tv.tv_sec
                except:
                    rc = -1

                if 0 != rc:
                    # on failure, revert to popen and execing sysctl
                    # '{ sec = 1399409948, usec = 0 } Tue May  6 13:59:08 2014\n'
                    s = os.popen('sysctl -n kern.boottime').read()
                    tstamp = float( s.split()[3].split(',')[0] )

            else:
                # GNU/Linux
                # read the "btime" entry from /proc/stat
                f = open("/proc/stat", "rb")
                stat = f.read()
                f.close()
                x = stat.find("\nbtime ")
                if x > 0:
                    x += 7  # skip btime label
                    y = stat.find('\n', x)
                    tstamp = float( stat[x:y] )

        except Exception:
            self.logger.info("get boottime failed, fallback to blade start")
        
        return tstamp;


    def GetAppTempDir (self):
        tmp = "/tmp"
        try:
            if sys.platform == 'win32':
                # ideally we'd use SHGetKnownFolderPath( FOLDERID_ProgramData )
                # if we can guarantee no pre-vista win32 issues in the site's
                # running python c code.  Instead use the older API for now.
                CSIDL_APPDATA = 26
                CSIDL_COMMON_APPDATA = 35
                getpath = ctypes.windll.shell32.SHGetFolderPathW
                getpath.argtypes = \
                    [ HWND, ctypes.c_int, HANDLE, DWORD, LPCWSTR ]
                buf = ctypes.wintypes.create_unicode_buffer(MAX_PATH)
                getpath(0, CSIDL_COMMON_APPDATA, 0, 0, buf)
                tmp = buf.value

            elif sys.platform == 'darwin':
                tmp = os.path.expanduser("~/Library/Application Support")

            else:  # linux
                tmp = "/var/tmp"

            tmp += "/Pixar/TractorBlade"
            if not os.access(tmp, os.W_OK):
                oldumask = os.umask(0)
                os.makedirs( tmp )
                os.umask(oldumask)

        except Exception, e:
            # it is acceptable for the directory to already exist
            if e[0] not in (errno.EEXIST, errno.ERROR_ALREADY_EXISTS):
                errclass, excobj = sys.exc_info()[:2]
                self.logger.warning("tmpdir %s - %s" % \
                                    (errclass.__name__, str(excobj)))

        return tmp

    def GetCheckpointFilename (self):
        return self.chkptfile

    def resolveCheckpointFilename (self, opts):
        #
        # The blade checkpoint file must be written someplace
        # where a restarted blade will find it again reliably.
        # The tricky bit is that more than one blade may be
        # running on each server, especially during testing,
        # and each may be connected to a different engine.
        # So the checkpoint needs to be specific to the "altname"
        # given to this blade and the engine from which it is
        # receiving jobs.
        #
        # chkpt.<enginehost_port>.<bladeAltname>.json
        # .../Pixar/TractorBlade/chkpt.tractor-engine_80.rack21A.json
        #
        out = None
        try:
            chkpt = self.GetAppTempDir()

            mh = opts.mtdhost.split('.')[0]
            bh = self.hostname.split('.')[0]
            pixarChkpt = "/chkpt." + \
                          mh + "_" + str(opts.mport) + \
                          "." + bh + ".json"
            chkpt += pixarChkpt
            try:
                self.MakeCheckpointDir( chkpt )

            except Exception, e:
                # it is acceptable for the directory to already exist
                if e[0] not in (errno.EEXIST, errno.ERROR_ALREADY_EXISTS):
                    raise

            # test the file for i/o
            f = open( chkpt, "a+b" )
            f.close()
            out = chkpt

        except Exception:
            self.logger.info("getCheckpointFilename: " + self.logger.Xcpt())

        return out


    def MakeCheckpointDir (self, fname):
        oldumask = os.umask(0)
        os.makedirs( os.path.dirname(fname) )
        os.umask(oldumask)


    def uniquifySequence(self, seq):
        # Remove duplicates from the inbound sequence.
        # Does not preserve order among retained items.
        return {}.fromkeys(seq).keys()


    def GetAvailableDisk (self, drivepath=None):
        """
        Returns the amount of currently free disk space on
        the drive containing 'drivepath' (e.g. "/").
        Units are (float) gigabytes.  If drivepath is None,
        than a typical platform root directory is chosen.
        """

        gbdisk = 0.0
        try:
            if sys.platform == 'win32':
                if not drivepath:
                    drivepath = u'c:\\'
                nbytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW (
                    ctypes.c_wchar_p( drivepath ),
                    None, None, 
                    ctypes.pointer(nbytes)
                )
                gbdisk = float(nbytes.value)

            else:
                # unix (recent linux and osx, with a recent python)
                if not drivepath:
                    drivepath = '/'
                s = os.statvfs( drivepath )
                gbdisk = float(s.f_bavail * s.f_frsize)

        except Exception:
            self.logger.debug("getAvailableDisk: " + self.logger.Xcpt())

        return gbdisk / (1024.0 * 1024.0 * 1024.0)


    def getPhysicalRAM (self):
        # physical RAM installed, in gigabytes
        gbram = 0.5
        try:
            if sys.platform == 'win32':
                # sets self.physRAM as side effect!
                self.getAvailableRAM_Windows()
                gbram = self.physRAM
            elif sys.platform == 'darwin':
                ram = ctypes.c_uint64(0)
                size = ctypes.c_size_t( ctypes.sizeof(ram) )
                if 0 == tr_ctypes_libc.sysctlbyname("hw.memsize", \
                        ctypes.byref(ram),ctypes.byref(size), None, 0):
                    gbram = float(ram.value) / (1024.0 * 1024.0 * 1024.0)
            else:
                # sets self.physRAM as side effect!
                self.getAvailableRAM_Linux()
                gbram = self.physRAM

        except Exception:
            self.logger.debug("getPhysicalRAM: " + self.logger.Xcpt())

        return gbram
        


    def GetAvailableRAM (self):
        # estimate of free memory, in gigabytes
        gbfree = 0.1234
        try:
            if sys.platform == 'win32':
                gbfree = self.getAvailableRAM_Windows()
            elif sys.platform == 'darwin':
                gbfree = self.getAvailableRAM_OSX()
            else:
                gbfree = self.getAvailableRAM_Linux()

        except Exception:
            self.logger.debug("getAvailableRAM: " + self.logger.Xcpt())

        return gbfree


    def getAvailableRAM_OSX (self):
        # extract free+inactive pages, return "free" gigabytes
        vmstat = subprocess.Popen(["/usr/bin/vm_stat"], stdout=subprocess.PIPE).communicate()[0]
        avail = 0.0
        for t in vmstat.split('\n'):
            m = t.split()
            if 3==len(m) and m[1] in ('free:', 'inactive:', 'speculative:'):
                avail += float(m[2])

        return avail * 3.8147E-6  # (4096 / (1024.0 * 1024.0 * 1024.0))


    def getAvailableRAM_Linux (self):
        # extract meminfo items (in kb), return "free" gigabytes
        f = open("/proc/meminfo", "r")
        mi = f.read()
        f.close()

        avail = 0.0
        for k in mi.split('\n'):
            m = k.split()
            if 3==len(m) and m[0] in ('MemFree:', 'Buffers:', 'Cached:'):
                avail += float(m[1])
            elif 3==len(m) and m[0] == 'MemTotal:':
                # side effect:  save physical RAM
                self.physRAM = \
                    math.ceil( float(m[1]) / (1024.0 * 1024.0) )

        return avail / (1024.0 * 1024.0)


    ## ------------------ windows methods --------------------- ##

    def getAvailableRAM_Windows (self):
        # Gets memory usage on Windows
        DWORDLONG = ctypes.c_uint64

        # class modeled after the MEMORYSTATUSEX struct in Winbase.h in
        # the Windows SDK.  See doc details in this msdn article:
        # http://msdn.microsoft.com/en-us/library/aa366770(VS.85).aspx
        class MEMORYSTATUSEX(ctypes.Structure):
           _fields_ = [('dwLength', DWORD),
                       ('dwMemoryLoad', DWORD),
                       ('ullTotalPhys', DWORDLONG),
                       ('ullAvailPhys', DWORDLONG),
                       ('ullTotalPageFile', DWORDLONG),
                       ('ullAvailPageFile', DWORDLONG),
                       ('ullTotalVirtual', DWORDLONG),
                       ('ullAvailVirtual', DWORDLONG),
                       ('ullAvailExtendedVirtual', DWORDLONG)]

        memStatus = MEMORYSTATUSEX()
        memStatus.dwLength = ctypes.sizeof(memStatus);

        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memStatus)) == 0:
            print ctypes.WinError()
        else:
            gbscale = (1024.0 * 1024.0 * 1024.0)

            # side effect:  save physical RAM
            self.physRAM = math.ceil( float(memStatus.ullTotalPhys) / gbscale )

            gbfree = float(memStatus.ullAvailPhys) / gbscale
            return gbfree


    def getProcessorUsage_Windows (self):
        # Gets CPU usage on Windows
        cpuUsage = 0.0

        # create a class modeled after the FILETIME struct in Winbase.h in
        # the Windows SDK.  See doc details in this msdn article:
        # http://msdn.microsoft.com/en-us/library/ms724284.aspx
        class FILETIME(ctypes.Structure):
            _fields_ = [('dwLowDateTime', DWORD),
                        ('dwHighDateTime', DWORD)]

        ftIdle = FILETIME()
        ftKernel = FILETIME()
        ftUser = FILETIME()

        if 0 == ctypes.windll.kernel32.GetSystemTimes( \
                                    ctypes.byref(ftIdle),
                                    ctypes.byref(ftKernel),
                                    ctypes.byref(ftUser) ):
            print(ctypes.WinError())
        else:
            # the FILETIME struct stores a high order 32-bit value and a low order 32-bit value
            # we need to combine the high and low order portions to create one unsigned 64-bit integer
            uIdle = ftIdle.dwHighDateTime << 32 | ftIdle.dwLowDateTime
            uKernel = ftKernel.dwHighDateTime << 32 | ftKernel.dwLowDateTime
            uUser = ftUser.dwHighDateTime << 32 | ftUser.dwLowDateTime

            if self.cpuOldIdle != None:
                uDiffIdle = uIdle - self.cpuOldIdle
                uDiffKernel = uKernel - self.cpuOldKernel
                uDiffUser = uUser - self.cpuOldUser    
                systime = uDiffKernel + uDiffUser

                if (systime != 0):
                    cpuUsage = float(systime - uDiffIdle) / (systime)
                    #print 'CPU Usage = %g' % (cpuUsage)

            # save these for next time
            self.cpuOldIdle = uIdle
            self.cpuOldKernel = uKernel
            self.cpuOldUser = uUser

        return cpuUsage


    def ResourceProbeForPIDs (self, cmdlist):
        '''Collect runtime resource usage for a list of subprocesses.'''
        if not cmdlist:
            return
        try:
            if "linux" == sys.platform[:5]:
                self.linuxResourceProbeForPIDs( cmdlist )
            elif "darwin" == sys.platform:
                self.osxResourceProbeForPIDs( cmdlist )
            elif "win32" == sys.platform:
                #self.windowsResourceProbeForPIDs( cmdlist )
                pass
        except Exception, e:
            self.logger.error("ResourceProbeForPIDs error: "+str(e))


    def linuxResourceProbeForPIDs (self, cmdlist):
        # examine /proc/<pid>/stat for tracking values

        now = time.time()
        MB = 1024.0 * 1024.0

        for cmd in cmdlist:
            pid = cmd.pid
            f = open("/proc/%d/stat" % pid, "rb")
            v = f.read().split()
            f.close()

            # Compute the total theoretical consumable cpu "time" in
            # seconds that was available over that last time interval,
            # it is the number of cpu cores times the interval in secs
            availableCpuTime = (now - cmd.lastStatTime) * self.nCPUs
            cmd.lastStatTime = now

            # now compute the percentage of total available "ticks"
            # that were consumed by this child process over the interval
            tusr = float(v[13]) / self.hostClkTck
            tsys = float(v[14]) / self.hostClkTck
            used = (tusr - cmd.tuser) + (tsys - cmd.tsys)
            cpu = used / availableCpuTime;

            # save for next iteration
            cmd.tuser = tusr
            cmd.tsys = tsys

            # get the memory usage stats
            vsz = float(v[22]) / MB             # bytes to mb
            rss = float(v[23]) * (4096.0 / MB)  # pages to mb

            if rss > cmd.maxRSS:
                cmd.maxRSS = rss
            if vsz > cmd.maxVSZ:
                cmd.maxVSZ = vsz
            if cpu > cmd.maxCPU:
                cmd.maxCPU = cpu

    def osxResourceProbeForPIDs (self, cmdlist):
        # for now, just exec 'ps' to get the values we need

        # handy constants, percent of one cpu -> fraction of all
        normCPU = 1.0 / (self.nCPUs * 100.0)
        kb2mb = 1.0 / 1024.0

        ps = [ "/bin/ps", "-opid,rss,vsz,%cpu", 
               "-p" + ",".join([str(c.pid) for c in cmdlist]) ]
        a = subprocess.check_output(ps)

        pdict = {}
        for line in a.split('\n')[1:]:
            v = line.split()
            if v:
                pid = int(v[0])
                rss = float(v[1]) * kb2mb
                vsz = float(v[2]) * kb2mb
                cpu = float(v[3]) * normCPU

                pdict[pid] = (rss, vsz, cpu)

        for cmd in cmdlist:
            if cmd.pid in pdict:
                rss, vsz, cpu = pdict[cmd.pid]

                if rss > cmd.maxRSS:
                    cmd.maxRSS = rss
                if vsz > cmd.maxVSZ:
                    cmd.maxVSZ = vsz
                if cpu > cmd.maxCPU:
                    cmd.maxCPU = cpu


    def windowsResourceProbeForPIDs (self, cmdlist):
        rss = 0
        vsz = 0
        cpu = 0

        for cmd in cmdlist:
            subprocessHandle = None
            # like: ctypes.windll.kernel32.GetCurrentProcess()
            # likely something in the python subprocess object
            if subprocessHandle:
                mem_struct = PROCESS_MEMORY_COUNTERS_EX()
                ret = ctypes.windll.psapi.GetProcessMemoryInfo(
                            subprocessHandle,
                            ctypes.byref(mem_struct),
                            ctypes.sizeof(mem_struct)
                        )
                if ret:
                    # units are bytes(?), we want MB
                    rss = mem_struct.PeakWorkingSetSize / (1024.0 * 1024.0)

## --------------------------------------------------- ##

if sys.platform == 'win32':
    class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
        """Used by GetProcessMemoryInfo"""
        _fields_ = [('cb', ctypes.c_ulong),
                    ('PageFaultCount', ctypes.c_ulong),
                    ('PeakWorkingSetSize', ctypes.c_size_t),
                    ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t),
                    ('PeakPagefileUsage', ctypes.c_size_t),
                    ('PrivateUsage', ctypes.c_size_t),
                   ]
## --------------------------------------------------------- ##
## --------------------------------------------------------- ##
import getpass

def TrGetProcessOwnerName ():
    unm = "unknown"
    try:
        # this can fail on windows service run as LocalSystem
        # or on unix if env vars are unavailable (boot of daemons?)
        unm = getpass.getuser()
    except:
        try:
            if sys.platform == 'win32':
                import ctypes
                dll=ctypes.windll.LoadLibrary("advapi32.dll")
                buff = ctypes.create_string_buffer('\0' * 256)
                bufflen = ctypes.c_long(256)
                fun = dll.GetUserNameA
                fun(buff, ctypes.byref(bufflen))
                unm = buff.value
            else:
                unm = str( os.getuid() )
        except:
            pass

    return unm

## --------------------------------------------------------- ##

