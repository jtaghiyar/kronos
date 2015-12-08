'''
Created on Mar 10, 2014

@author: jtaghiyar
'''

import os, glob
from datetime import datetime
from helpers import make_dir


class RunManager(object):
    '''
    takes care of run_id, making and managing directories and sentinel files, ...
    '''

    def __init__(self, run_id=None, pipeline_name=None, working_dir=None):
        if run_id is None:
            run_id = self._get_run_id()

        if pipeline_name is None:
            pipeline_name = 'pipeline'

        if working_dir is None:
            working_dir = os.getcwd()

        make_dir(working_dir)

        self.run_id        = run_id
        self.sample_ids    = list()
        self.pipeline_name = pipeline_name
        self.working_dir   = os.path.join(os.path.abspath(working_dir))
        self.pipeline_dir  = os.path.join(self.working_dir, self.pipeline_name)
       
        make_dir(self.pipeline_dir)

        self.logs_dir      = self._make_dir('logs')
        self.outputs_dir   = self._make_dir('outputs')
        self.results_dir   = self._make_dir('outputs/results')
        self.scripts_dir   = self._make_dir('scripts')
        self.sentinels_dir = self._make_dir('sentinels')


    def get_filename_prefix(self, task_name):
#         return '_'.join([component_name, task_name, self.pipeline_name, self.run_id])
        return task_name.strip('_')


    def update_file_names(self, prefix, pattern = '*'):
        dirpath = self.outputs_dir

        for filename in glob.glob(os.path.join(dirpath, pattern)):
            newname = prefix + os.path.basename(filename)
            newname = os.path.join(dirpath, newname)
            os.rename(filename, newname)


    def add_sample(self, sample_id):
        self.sample_ids.append(sample_id)


    def get_sentinel_file_name(self, task_name):
        temp_name  = self.get_filename_prefix(task_name) + '_sentinel_file'
        file_name  = os.path.join(self.sentinels_dir, temp_name)

        return file_name


    def sentinel_file_exists(self, component_name, task_name, predecessor_task_names):
        curr_sentinel_file = self.get_sentinel_file_name(task_name)
        
        ## check if all the predecessor sentinel files exist and record
        ## their modification time.
        mtimes = []
        for ptn in predecessor_task_names:
            filename = self.get_sentinel_file_name(ptn)
            if not os.path.exists(filename):
                return False, "Missing predecessor sentinel file %s" % filename
            mtimes.append(os.path.getmtime(filename))

        if not os.path.exists(curr_sentinel_file) :
            return True, "Missing sentinel file %s" % curr_sentinel_file
        
        else:
            ## check if the current sentinel is older than any of 
            ## the predecessor sentinels
            curr_mt = os.path.getmtime(curr_sentinel_file)
            if any(map(lambda mt: mt > curr_mt, mtimes)):
                return True, "Outdated sentinel file %s" % curr_sentinel_file
            else:
                return False, "Up-to-date sentinel files exist for task %s" % task_name
        

    def generate_sentinel_file(self, task_name):
        sentinel_file = self.get_sentinel_file_name(task_name)
        open(sentinel_file, 'w')


    def generate_script(self, task, chunk=None, boilerplate=None):
        '''
        generate component run script including the export commands
        for its required environment variables
        '''

        component = task.component
        task_name = task.task_name

        ## make a file name
        temp_name  = self.get_filename_prefix(task_name) + '.sh'
        file_name  = os.path.join(self.scripts_dir, temp_name)

        ## get commands and environment variables from component
        cmd, cmd_args = component.make_cmd(chunk)
        env_vars = component.env_vars

        with open(file_name, 'w') as run_script:
            run_script.write('#! /bin/bash' + '\n')
            if env_vars is not None:
                self._write_env_vars(env_vars, run_script)
            if boilerplate is not None:
                self._write_boilerplate(boilerplate, run_script)

            run_script.write(cmd + ' ' +' '.join(map(str, cmd_args)))

        ## make it executable
        os.chmod(file_name, 0755)

        return os.path.abspath(file_name)

    def _write_env_vars(self, env_vars, out_stream):
        for k, v in env_vars.iteritems():
            if isinstance(v, list):
                for vi in v:
                    out_stream.write('export %s=%s:$%s\n' % (k, vi, k))

            else:
                out_stream.write('export %s=%s:$%s\n' % (k, v, k))
    
    def _write_boilerplate(self, boilerplate, out_stream):
        if os.path.isfile(boilerplate):
            with open(boilerplate, 'r') as bp:
                lines = bp.readlines()
                out_stream.writelines(lines)
        else:
            out_stream.write(boilerplate)
        out_stream.write('\n')

    def _get_run_id(self):
        return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


    def _make_dir(self, dir_name):
        temp_dir = os.path.join(self.pipeline_dir, dir_name)
        make_dir(temp_dir)
        
        return temp_dir
