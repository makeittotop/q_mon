#
# TrStatusFilter - a class that collects current system status
# and determines whether it is appropriate to ask the engine
# for new tasks.  Can be subclassed by site-defined override
# filters, using a local copy of TractorSiteStatusFilter.py.
#
# That's worth repeating:
# DO NOT COPY or modify this file to get site-specific
# custom filtering of blade state or custom readiness tests!
# INSTEAD:  copy TractorSiteStatusFilter.py into the location
# indicated by SiteModulesPath in blade.config, and make local
# changes to that copy.  You can implement your own filter methods
# there, call the superclass methods (from this file) while also
# adding your own pre/post processing in the subclass.
# 
#
# ____________________________________________________________________ 
# Copyright (C) 2010-2014 Pixar Animation Studios. All rights reserved.
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

import logging


## ------------------------------------------------------------- ##
class TrStatusFilter ( object ):

    def __init__(self):
        # look up the global blade logger object
        self.logger = logging.getLogger("tractor-blade")


    ## ------------------ basic state tests -------------------- ##

    def FilterBasicState(self, stateDict, now):
        """ Makes custom modifications to the stateDict.
            The inbound dict will contain basic blade
            configuration state at the time this routine
            is called.  NOTE: the stateDict may be "None"
            if the blade has not yet been able to download
            the blade.config profiles from the engine.

            These "cheap and easy to acquire" settings will be
            used for quick early-out tests prior to the more 
            expensive dynamic status-gathering and testing phase.
        """
        pass  # default filter makes no changes to curstate


    def TestBasicState (self, stateDict, now):
        #
        # by default, we don't ask for work if still waiting
        # to download a valid profile definition
        #
        if not stateDict:
            # no place to write our excuse string either
            return False

        result = False # pessimistic by nature

        try:
            if not stateDict["isInService"]:
                # When not in service, we can become relatively dormant.
                # Internally the profile will occasionally check for updates.
                stateDict["excuse"] = "profile specifies 'Not in Service'"

            elif len( stateDict["exclusiveKeys"] ) > 0:
                # a cmd using an "exclusive" service key is running,
                # so we decline to ask for more 
                stateDict["excuse"] = "exclusiveKey active: " + \
                                    ','.join(stateDict["exclusiveKeys"])

            elif stateDict["slotsAvailable"] <= 0:
                # this blade's concurrent-cmd limit reached
                stateDict["excuse"] = \
                    "no free slots (%d)" % stateDict["slotsInUse"]

            else:
                result = True  # we should do expensive checks

        except Exception:
            stateDict["excuse"] = "TestBasicState failed"
        
        return result

    ## ------------------ dynamic state tests -------------------- ##

    def FilterDynamicState (self, stateDict, now):
        pass

    def TestDynamicState (self, stateDict, now):
        #
        #
        #
        result = False
        if stateDict["cpuLoad"] > stateDict["maxLoad"]:
            stateDict["excuse"] = "MaxLoad(%.3g > %.3g) exceeded" % \
                            (stateDict["cpuLoad"], stateDict["maxLoad"])

        elif stateDict["freeRAM"] < stateDict["minRAM"]:
            stateDict["excuse"] = "MinRAM(%.3g < %.3g GB) insufficient" % \
                            (stateDict["freeRAM"], stateDict["minRAM"])

        elif stateDict["freeDisk"] < stateDict["minDisk"]:
            stateDict["excuse"] = "MinDisk(%.3g < %.3g GB) insufficient" % \
                            (stateDict["freeDisk"], stateDict["minDisk"])

        else:
            result = True  # can proceed with task request!

        return result


    ## ------- notifier methods for subprocess begin/end ------- ##
    # These methods are the default site-defined callbacks that are
    # triggered when tractor-blade launches a new command, and also when
    # each command subprocess exits.  The idea is to allow sites to
    # implement custom logging at command launch and exit events, and
    # also to allow for the collection of process information for purposes
    # external to Tractor.  It may also provide a context for custom
    # pre/post processing around a command execution on each blade.  

    def SubprocessStarted (self, cmd):
        '''
        Called after a command has been successfully launched.
        '''
        guise = ""
        if cmd.guise:
            guise = "(as %s) " % cmd.guise

        self.logger.info("+pid[%d] %s %s'%s' #%d" % \
                (cmd.pid, cmd.logref, guise, cmd.app, cmd.slots))

        if cmd.launchnote:
            self.logger.debug(" note: %s %s" % (cmd.logref, cmd.launchnote))


    # ---------------------------- #
    def SubprocessFailedToStart (self, cmd):
        '''
        Called after a command exec has failed, usually due
        to path problems or mismatched platforms.
        '''
        guise = ""
        emsg = "failed to launch"
        if cmd.launchnote:
            emsg = cmd.launchnote
        if cmd.guise:
            guise = " (as %s)" % cmd.guise

        self.logger.info("launch FAILED%s: %s, [%s] %s" % \
                        (guise, emsg, cmd.argv[0], cmd.logref))


    # ---------------------------- #
    def SubprocessEnded (self, cmd):
        '''
        Called after a command process has stopped running, either
        because it exitted on its own or because it was killed.
        '''
        self.logger.info("-pid[%d] %s rc=%d %.2fs #%d%s" % \
            (cmd.pid, cmd.logref, cmd.exitcode, cmd.elapsed, cmd.slots,
             (" (chkpt)" if cmd.yieldchkpt else "")))

## ------------------------------------------------------------- ##
