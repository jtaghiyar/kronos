'''
Created on May 9, 2014

@author: jtaghiyar
'''
import os, sys
import subprocess as sub
from multiprocessing import Queue
from helpers import JobIdNotFound, make_dir
from random import randint
from time import sleep
import re

class DrmaaJob(object):
    
    """
    cluster job template.
    """
    
    def __init__(self, drmaa_session, cmd, cmd_args, mem, h_vmem, ncpus,
                 job_name=None, log_dir=None, working_dir=None, qsub_options=None):

        job_template = drmaa_session.createJobTemplate()
        job_template.remoteCommand = cmd
        job_template.args = cmd_args
        job_template.joinFiles = True
        job_template.environment = {'BASH_ENV':'~/.bashrc'}
        
        if qsub_options is None:
            qsub_options  = '-l mem_free={mem},mem_token={mem},h_vmem={h_vmem} '
            qsub_options += '-pe ncpus {num_cpus} -j y -w n -V' 
        job_template.nativeSpecification = qsub_options.format(mem=mem, 
                                                               h_vmem=h_vmem,
                                                               num_cpus=ncpus)
        
        if job_name is not None:
            job_template.jobName = job_name
            
        if working_dir is not None:
            job_template.workingDirectory = working_dir
            
        if log_dir is not None:
            job_template.errorPath  = ':' + log_dir
            job_template.outputPath = ':' + log_dir

        self.cmd = cmd
        self.cmd_args = cmd_args
        self.job_name = job_name
        self.job_template = job_template

        
class DrmaaJobManager(object):
    
    """
    cluster job manager, i.e. drmaa_wrapper.
    """

    def __init__(self, drmaa_library_path, log_dir=None, working_dir=None, qsub_options=None):

        if working_dir is not None:
            make_dir(working_dir)
        else:
            working_dir = os.getcwd()

        if log_dir is not None:
            make_dir(log_dir)
        else:
            log_dir = working_dir
        
        ## export DRMAA_LIBRARY_PATH to import drmaa 
        os.environ['DRMAA_LIBRARY_PATH'] = os.path.join(os.environ['SGE_ROOT'], drmaa_library_path)
        import drmaa

        self.job_ids = Queue()
        self.drmaa = drmaa
        self.log_dir = os.path.abspath(log_dir)
        self.working_dir = os.path.abspath(working_dir)
        self.qsub_options = qsub_options

    def run_job(self, cmd, cmd_args="", mem='10G', h_vmem=None, ncpus=1, job_name=None):
        """submit a single job to the cluster and wait to finish."""
        if h_vmem is None:
            h_vmem = self._get_hvmem(mem)
            
        with self.drmaa.Session() as s:
            job = DrmaaJob(s, cmd, cmd_args, mem, h_vmem, ncpus, job_name,
                             self.log_dir, self.working_dir, self.qsub_options)
            job_id = s.runJob(job.job_template)
            print 'job %s ("%s") has been submitted' % (job_id, job.job_name)
            self.job_ids.put(job_id)
            
            ## wait for the job
            try:
                return_code = self.wait(s, job_id)
                print 'job %s finished with return code %s' % (job_id, return_code)
            except:
                print 'an error happened in job %s. Deleting the job from the queue...' % (job_id)
                s.control(job_id, self.drmaa.JobControlAction.TERMINATE)
                raise
            finally:
                s.deleteJobTemplate(job.job_template)
        return return_code

    def wait(self, drmaa_session, job_id):
        """
        self.drmaa.JobState.UNDETERMINED: 'status of job %s cannot be determined',
        self.drmaa.JobState.QUEUED_ACTIVE: 'job %s is queued and active',
        self.drmaa.JobState.SYSTEM_ON_HOLD: 'job %s is queued and in system hold',
        self.drmaa.JobState.USER_ON_HOLD: 'job %s is queued and in user hold',
        self.drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job %s is queued and in user and system hold',
        self.drmaa.JobState.RUNNING: 'job %s is running',
        self.drmaa.JobState.SYSTEM_SUSPENDED: 'job %s is system suspended',
        self.drmaa.JobState.USER_SUSPENDED: 'job %s is user suspended',
        self.drmaa.JobState.DONE: 'job %s finished normally',
        self.drmaa.JobState.FAILED: 'job %s finished, but failed',
        """
     
        normal_status = (
                         self.drmaa.JobState.QUEUED_ACTIVE,
                         self.drmaa.JobState.RUNNING
#                          self.drmaa.JobState.UNDETERMINED
                         )
        hold_status = (
                       self.drmaa.JobState.SYSTEM_ON_HOLD,
                       self.drmaa.JobState.USER_ON_HOLD,
                       self.drmaa.JobState.USER_SYSTEM_ON_HOLD,
                       self.drmaa.JobState.SYSTEM_SUSPENDED,
                       self.drmaa.JobState.USER_SUSPENDED
                       )
         
        exit_status = drmaa_session.jobStatus(job_id)
        while(exit_status in normal_status):
            sleep(randint(30, 180))
            exit_status = drmaa_session.jobStatus(job_id)
             
        if exit_status == self.drmaa.JobState.DONE:
            return 0
        elif exit_status in hold_status:
            return 1
        elif exit_status == self.drmaa.JobState.FAILED:
            return -1
        else:
            return 2

#     def wait(self, drmaa_session, job_id):
#         """wait for the given job_id in the given session to finish."""
#         WAIT_TIME = 3
#         while True:
#             try:
#                 ## use WAIT_TIME to enable catching the KeyboardInterrupt 
#                 ret_val = drmaa_session.wait(job_id, WAIT_TIME)
#                 return ret_val.exitStatus
#  
#             except self.drmaa.errors.ExitTimeoutException:
#                 sleep(randint(30, 180))
#                 pass
#             except:
#                 raise

    def kill_job(self, job_id):
        """kill the job with given job_id."""
        self.drmaa.Session.control(job_id,
                                   self.drmaa.JobControlAction.TERMINATE)
#         cmd = "qdel %s" % (job_id)
#         os.system(cmd)
        
    def kill_all(self):
        """kill all the jobs in the queue."""
        while not self.job_ids.empty():
            job_id = self.job_ids.get()
            self.kill_job(job_id)
#             cmd = "qdel %s" % (job_id)
#             os.system(cmd)

    @staticmethod
    def _get_hvmem(mem):
        r = r"\d+.\d+|\d+"
        m = re.findall(r, mem)[0]
        s = re.split(r, mem)[1]
        h_vmem = str(float(m) * 1.2) + s
        return h_vmem
    
    
class SgeJob(object):
    
    """
    SGE cluster job template.
    """
    
    def __init__(self, cmd, cmd_args, mem, h_vmem, num_cpus, job_name=None,
                 log_dir=None, working_dir=None, qsub_options=None):
    
        if qsub_options is None:
            qsub_options = ' -l mem_free={mem},mem_token={mem},h_vmem={h_vmem}'
            qsub_options += ' -pe ncpus {num_cpus} -j y -w n -V -b y'
        else:
            qsub_options += ' -b y'    

        if job_name is not None:
            qsub_options += ' -N {job_name}'

        if log_dir is not None:
            qsub_options += ' -e {log_dir} -o {log_dir}'
                
        if working_dir is not None:
            qsub_options += ' -wd {working_dir}'
        
        input_opt = { 'mem': mem,
                      'h_vmem': h_vmem,
                      'num_cpus': num_cpus,
                      'job_name': job_name,
                      'log_dir': log_dir,
                      'working_dir': working_dir
#                       'environment': environment
                     }
        cmd_args = ' '.join(map(str, cmd_args))
        self.qopt = qsub_options.format(**input_opt)
        self.qcmd = 'qsub {0} {1} {2}'.format(self.qopt, cmd, cmd_args)
        self.job_name = job_name
    
class SgeJobManager(object):
    
    """
    SGE cluster job manager.
    """
    
    def __init__(self, log_dir=None, working_dir=None, qsub_options=None):
        if working_dir is not None:
            make_dir(working_dir)
        else:
            working_dir = os.getcwd()

        if log_dir is not None:
            make_dir(log_dir)
        else:
            log_dir = working_dir
        
        self.job_ids = Queue()
        self.log_dir = os.path.abspath(log_dir)
        self.working_dir = os.path.abspath(working_dir)
        self.qsub_options = qsub_options
    
    def run_job(self, cmd, cmd_args="", mem='10G', h_vmem=None, ncpus=1,
                job_name=None):
        """submit a single job to the cluster and wait to finish."""
        if h_vmem is None:
            h_vmem = self._get_hvmem(mem)
        
        if cmd_args and not isinstance(cmd_args, (list, tuple)):
            cmd_args = [cmd_args]
        
        sj = SgeJob(cmd, cmd_args, mem, h_vmem, ncpus, job_name,
                    self.log_dir, self.working_dir, self.qsub_options)
        job_id = self._run_job(sj)
        print 'Your job %s ("%s") has been submitted' % (job_id, job_name)
        self.job_ids.put(job_id)
        
        ## wait for the job
        try:
            return_code = -1
            return_code = self.wait(job_id)
            msg = 'job {0} ("{1}") finished with return code {2}'
            msg = msg.format(job_id, job_name, return_code)
            print msg
        except:
            msg = 'An error happened in job {0} ("{1}").'
            msg += ' Deleting the job from the queue ...'
            msg = msg.format(job_id, job_name)
            print msg
            self.kill_job(job_id)
            raise

        return return_code
        
    def wait(self, job_id):
        """wait for the job_id until is no longer in queue."""
        timeout = randint(30, 180)
        while True:
            if self.isinqueue(job_id):
                ##sleep for timeout seconds to avoid overwhelming the qmaster.
                sleep(timeout)
            else:
                break
        try:
            return_code = self.qacct(job_id)
        except JobIdNotFound:
            ##wait for the database to update
            sleep(timeout)
            return_code = self.qacct(job_id)
        except JobIdNotFound:
            print 'qacct failed to get return_code for job_id: %s' % (job_id)
            return_code = -2 
        
        return return_code
    
    @staticmethod
    def kill_job(job_id):
        kill_cmd = 'qdel %s' % (job_id)
        os.system(kill_cmd)
        
    def kill_all(self):
        """kill all the jobs in the queue."""
        while not self.job_ids.empty():
            job_id = self.job_ids.get()
            self.kill_job(job_id)
    
    @staticmethod
    def isinqueue(job_id):
        """check if the job_id is in queue regardless of its state."""
        cmd = 'qstat -j %s' % (job_id)
        proc = sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, shell=True)
        out, err = proc.communicate()
        return True if proc.returncode == 0 else False
            
    @staticmethod        
    def qacct(job_id):
        cmd = 'qacct -j %s' % (job_id)
        proc = sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode == 0:
            fields = out.split('\n')
        else:
            raise JobIdNotFound(job_id)
        
        for f in fields:
            if f.startswith('exit_status'):
                exit_status = int(f.strip().split()[1])
            elif f.startswith('failed'):
                failed = int(f.strip().split()[1])
                
        return -1 if failed != 0 else exit_status 

    def _run_job(self, job):
        """submit the job to the SGE cluster."""
        proc = sub.Popen(job.qcmd, stdout=sub.PIPE, stderr=sub.PIPE,
                         shell=True)
        out, err = proc.communicate()
        if proc.returncode != 0:
            msg = 'Failed to submit the job {0} ("{1}") due to error:\n {2}'
            msg = msg.format(proc.pid, job.job_name, err)
            raise Exception(msg)
        
        ##parse out the job_id
        m = re.search(r'Your job [0-9]* \("', out)
        m = m.group(0) 
        job_id = int(m.strip().split()[2])
        return job_id

    @staticmethod
    def _get_hvmem(mem):
        r = r"\d+.\d+|\d+"
        m = re.findall(r, mem)[0]
        s = re.split(r, mem)[1]
        h_vmem = str(float(m) * 1.2) + s
        return h_vmem
    
    
class LocalJob(object):
    
    """
    local job template.
    """
    
    def __init__(self, cmd, cmd_args, job_name=None):
        
        if cmd_args is not None:
            cmd = cmd + " " + " ".join(map(str,cmd_args))
            
        self.cmd = cmd
        self.cmd_args = cmd_args
        self.job_name = job_name


class LocalJobManager(object):
    
    """local job manager."""
    
    def __init__(self, log_dir=None, working_dir=None):

        if working_dir is not None:
            make_dir(working_dir)
        else:
            working_dir = os.getcwd()
        
        if log_dir is not None:
            make_dir(log_dir)
        else:
            log_dir = working_dir
        
        self.job_ids = []
        self.log_dir = os.path.abspath(log_dir)
        self.working_dir = os.path.abspath(working_dir)

    def run_job(self, cmd, cmd_args=None, job_name=None):
        """run a single job locally."""
        ## set the current working directory and back up the old one
        cwdback = os.getcwd()
        os.chdir(self.working_dir)
        
        job = LocalJob(cmd, cmd_args, job_name)
        proc = sub.Popen(job.cmd, stdout=sub.PIPE, stderr=sub.PIPE, shell=True)
        job_id = proc.pid
        print 'job %s ("%s") has been launched locally' % (proc.pid, job.job_name)
        self.job_ids.append(job_id)
        
        ## wait for the job
        try:
            cmdout, cmderr = proc.communicate()
            self._log_run(job.cmd, cmdout, cmderr, job.job_name, job_id)
        except:
            print 'an error happened in job %s. Killing the job...' % (job_id)
            self.kill_job(job_id)
            raise
        finally:
            os.chdir(cwdback)
        return proc.returncode

    def kill_job(self, job_id):
        """kill the job with given job_id."""
        cmd = "kill %s" % (job_id)
        os.system(cmd)
        
    def kill_all(self):
        """kill all the jobs in the queue."""
        for job_id in self.job_ids:
            cmd = "kill %s" % (job_id)
            os.system(cmd)
    
    def _log_run(self, cmd, cmdout, cmderr, job_name, pid):
        """log the local run."""
        pid = str(pid)

        if job_name is not None:
            logfile = '_'.join(['local_job', job_name, pid, '.log'])
        else: 
            logfile = '_'.join(['local_job', pid, '.log'])
            
        logfile = os.path.join(self.log_dir, logfile)

        with open(logfile, 'w') as logfile:
            logfile.write("#job name: %s with job id: %s\n" % (job_name, pid))
            logfile.write("#command\n")
            logfile.write(cmd)
            logfile.write("\n")
            logfile.write("#cmdout\n")
            logfile.write(cmdout)
            logfile.write("\n")
            logfile.write("#cmderr\n")
            logfile.write(cmderr)
            logfile.write("\n")
