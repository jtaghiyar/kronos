'''
Created on Apr 16, 2014

@author: jtaghiyar
'''

import os
import subprocess as sub
from plumber import Plumber
from job_manager import LocalJobManager
from workflow_manager import WorkFlow
from helpers import trim, make_dir, export_to_environ 


class ComponentAbstract(object):
    
    """
    component template.
    """
    
    def __init__(self, component_name, component_parent_dir=None, seed_dir_name=None):
        ''' 
        initialize general attributes that each component must have. 
        '''

        ## export component parent directory to the PYTHONPATH env var        
        if component_parent_dir is not None:
            export_to_environ(component_parent_dir, 'PYTHONPATH')
        
        ## import modules of the component, i.e. component_reqs and component_params.
        ## if component_parent_dir==None, then components directory must have been exported to 
        ## the PYTHONPATH env var beforehand.
        list_of_modules = ['component_' + x for x in['reqs', 'params']]
        m = __import__(component_name, globals(), locals(), list_of_modules, -1)

        if component_parent_dir is None:
            component_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(m.__file__)))
       
        if seed_dir_name is None:
            seed_dir_name = 'component_seed'
        
        ## The component_ui is NOT imported, since all the input arguments should be passed to 
        ## the component_main from config file via updating self.args attribute that happens in
        ## the corresponding task of the component. Therefore, an empty namespace is initialized
        ## here.
        import argparse
        parser = argparse.ArgumentParser()
        args, _ = parser.parse_known_args()
#         args.__dict__['return_value'] = None
        
        ## general attribute
        self.component_name = component_name
        self.component_dir  = component_parent_dir
        self.seed_dir       = os.path.join(self.component_dir, component_name, seed_dir_name)
        
        ## modules and args
        self.args             = args
        self._modules         = m
        self.component_reqs   = self._modules.component_reqs
        self.component_params = self._modules.component_params
                
        ## from the component_reqs
        self.env_vars        = self.component_reqs.env_vars
        self.memory          = self.component_reqs.memory
        self.parallel        = self.component_reqs.parallel
        self.requirements    = self.component_reqs.requirements.copy()
        self.seed_version    = self.component_reqs.seed_version
        self.version         = self.component_reqs.version

    def run(self):
        """run component via system command line locally."""
        cmd, cmd_args = self.make_cmd()
        ljm = LocalJobManager()
        ljm.run_job(cmd, cmd_args, self.component_name)
    
    def focus(self, cmd, cmd_args, chunk):
        "update the cmd and cmd_args for each chunk."
        
        raise NotImplementedError("focus method called before implementation")
        return cmd, cmd_args
    
    def make_cmd(self, chunk=None):
        """make a command."""
        cmd = None
        cmd_args = None
        
        raise NotImplementedError("make_cmd method called before implementation")
        return cmd, cmd_args
    
    def test(self):
        """run unittest of the component."""
        raise NotImplementedError("test method called before implementation")  


class Task(object):
    
    """
    Wrap one component for the following purposes:
    1. to update the args passed to the component via command line.
    2. to update the requirements of the component given in the config file.
    3. to give access to the 'input_files', 'output_files', 
       'input_params', 'return_values' and 'input_arguments' of the component.
    """

    def __init__(self, task_name, component):
        self.task_name = task_name
        self.component = component
        
    def update_comp_args(self, **kwargs):
        """Update self.component.args, i.e. overwrite argument specified vi command line.
        This can help pass the previous task's results to the parameters 
        of the current task.
        """
        ## change the Namespace object to dictionary
        args_dict = vars(self.component.args) 
        
        if kwargs is not None:
            kwargs = trim(kwargs, '__pipeline__')
            args_dict.update(kwargs)
    
    
    def update_comp_reqs(self, reqs_dict):
        """Update self.component.requirements dictionary if there are new 
        values given in the config file, or keep the default otherwise.
        """
        ## do not update the default value of a requirement
        ## if it is not changed in the config file
        ## or it is not one of the requirements of the components
        d = {k:v for k,v in reqs_dict.iteritems()
             if v is not None and k in self.component.requirements.keys()}
        
        self.component.requirements.update(d)

    
    def update_comp_env_vars(self, env_vars):
        """update the environment variables with values from the config file."""
        if not self.component.env_vars:
            self.component.env_vars = env_vars
        else:
            self.component.env_vars.update(env_vars)


    def update_comp_output_filenames(self, prefix, working_dir=None, no_prefix=False):
        """update the output file names by prepending the prefix to their names."""
        output_file_params = self.component.component_params.output_files.keys()
        
        ## change the Namespace object to dictionary
        args_dict = vars(self.component.args)
        
        wd = os.getcwd()
        if working_dir:
            os.chdir(working_dir)
            
        for param in output_file_params:
            value = args_dict.get(param)
            if value is not None:
                dirname = os.path.dirname(value)
                self._make_dirs(dirname)

                ## prepend filenames with the given prefix
                old_filename = os.path.basename(value)  
                if old_filename:
                    if no_prefix:
                        new_filename = old_filename
                    else:
                        new_filename = '_'.join([prefix, old_filename])
                        
                    args_dict[param] = os.path.join(dirname, new_filename)
                else:
                    args_dict[param] = dirname
        os.chdir(wd)

    def _make_dirs(self, path):
        """make dirs using os.makedirs"""
        if not path:
            return
        try:
            os.makedirs(path)
        except OSError as e:
            if e.strerror == 'File exists':
                pass
            else:
                raise
        

class Pipeline(object):
    '''
    a pipeline could be composed of one or more ruffus task
    that can be run as an independent entity provided that proper input/output
    arguments are passed to it.
    '''

    def __init__(self, pipeline_name, config_file, script_dir=os.getcwd(), sample_id=None):
        self.pipeline_name = pipeline_name
        self.config_file = config_file
        self.script_dir = script_dir
        self.sample_id = sample_id
        make_dir(self.script_dir)

        ## path to where the resultant pipeline script is written
        self.pipeline_script = os.path.join(self.script_dir, self.pipeline_name+'.py')

        ## use the WorkFlow to parse/make the config file
        self.wf = WorkFlow(config_file)
        
        ## holds the starting point of the sub pipeline, key:tag value:task_object
        self.start_task = {}

        ## holds the end point of the sub pipeline, key:tag value:task_object
        self.stop_task = {}

        ## list of all the inputs to the pipeline, i.e. set of the inputs of 
        ## all the root tasks. A dict with k:input_params and v:input_arguments
        self.inputs = {}

    def make_script(self, sample_id):
        """run the plumber and make a python script for the pipeline."""
        with open(self.pipeline_script, 'w') as ps:
            plumber = Plumber(ps, self.wf)
            plumber.make_script(sample_id)

    def run(self):
        try:
            ##TODO: this part is incomplete
            ## Technically, a pipeline is a script, and we run the 
            ## script here using a LocalJobManager
            cmd  = 'python {}'.format(self.pipeline_script)
            proc = sub.Popen(cmd, shell=True)
            cmdout, cmderr = proc.communicate()
            print cmdout, cmderr
            
#             ljm = LocalJobManager(logs_dir, results_dir)
#             ljm.run_job(cmd=cmd)


        except KeyboardInterrupt:
            print 'KeyboardInterruption in main'
            self.kill()
            raise

    def kill(self):
        """kill all the jobs."""
        pass

    def add_component(self, component_name, component_parent_dir):
        pass

    def add_task(self, task_name, component):
        """add task object to the list of tasks."""
        task = Task(task_name, component)
        self.tasks[task_name] = task

    def get_inputs(self):
        """get the list of all input file parameters of all the root
        components in the pipeline.
        """
        return self.tasks['root'].input_files

    def update_pipeline_script_args(self, args_namespace):
        """update args namespace of the pipeline script."""
        ## change the Namespace object to dictionary
        args_dict = vars(args_namespace)
        
        ##TODO: make proper dictionary from the values that
        ## needs to be passed to the pipeline script
        kwargs = None
        
        args_dict.update(kwargs)

    def update_components_args(self):
        """update all the arguments of all the components in the pipeline.
        It is equivalent to running __TASK___task.update_comp_args()
        method over each of the components in the pipeline.
        """
        pass

    def update_components_reqs(self):
        """update all the requirements of all the components in the pipeline.
        It is equivalent to running __TASK___task.update_comp_reqs()
        method over each of the components in the pipeline.
        """
        pass

    def import_python_modules(self):
        """import required python modules for the pipeline to run."""    
        pass

    def import_factory_modules(self):
        """import required factory modules for the pipeline to run."""
        pass

    def set_start_task(self, task_name):
        self.start_task = self.tasks[task_name]

    def set_stop_task(self, task_name):
        self.stop_task = self.tasks[task_name]
