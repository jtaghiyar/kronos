"""
Created on Mar 10, 2014

@author: jtaghiyar
"""

import os
import subprocess as sub
from kronos_version import kronos_version
from utils import Pipeline
from helpers import make_dir, Configurer
from workflow_manager import WorkFlow, WorkFlowManager
from plumber import Plumber
from string import Template
from tempfile import NamedTemporaryFile as ntf
import logging

logging.basicConfig(format='%(asctime)s %(message)s', 
                    level=logging.DEBUG)

class Factory(object):

    """
    create, run, manage pipelines.
    """

    def __init__(self, args):
        """initialize."""
        self.args = args
        self.pipelines = []
        self.c = Configurer()
        make_dir(self.args.working_dir)
        
    def make_config(self):
        """make a yaml config file."""
        file_name = os.path.join(self.args.working_dir, self.args.output_filename + '.yaml')
        config_dict = Configurer.make_config_dict(self.args.components)
        Configurer.print2yaml(config_dict, file_name)
        
    def init_pipeline(self):
        """initialize a new pipeline."""
        ## update __SAMPLES__, __SHARED__ and __GENERAL__ sections.
        self.c.config_file = self.args.config_file
        if self.args.input_samples:
            self.c.update_sample_section(self.args.input_samples)
        if self.args.setup_file:
            self.c.update_shared_section(self.args.setup_file)
            
        ## make a copy of the updated config file in the working directory.
        updated_cfgfile = os.path.basename(self.args.config_file).split('.yaml')[0]
        updated_cfgfile = os.path.join(self.args.working_dir, updated_cfgfile + '_kronos.yaml')
        Configurer.print2yaml(self.c.config_dict, updated_cfgfile)

        ## create a work flow from updated config file
        wf = WorkFlow(updated_cfgfile)
        samples = wf.get_samples()
        if not samples:
            self._make_intermediate_pipeline(self.args.pipeline_name, updated_cfgfile, None)
        
        else:
            for sample_id, sample_dict in samples.iteritems():
                new_config_file = self._make_intermediate_config_file(sample_id, sample_dict)
                pipeline_name = sample_id + '_' + self.args.pipeline_name
                self._make_intermediate_pipeline(pipeline_name, new_config_file, sample_id)
        
        self._paste_pipelines(updated_cfgfile)
    
    def run_pipeline(self):
        """run Kronos-made pipeline with optional initialization."""
        if self.args.config_file:
            if not self.args.pipeline_name:
                bname = os.path.basename(self.args.config_file)
                self.args.pipeline_name = os.path.splitext(bname)[0]
            
            self.init_pipeline()
            self.args.kronos_pipeline = os.path.join(self.args.working_dir,
                                                     self.args.pipeline_name + '.py')
        
        ## TODO: check if the -k input has been generated with kronos.
        cmd = "{python_installation} {kronos_pipeline} -b {job_scheduler} "
        cmd += "-c {components_dir} -d {drmaa_library_path} -j {num_jobs} "
        cmd += "-n {num_pipelines} -p {python_installation} -w {working_dir}"
        cmd = cmd.format(**vars(self.args))
        if self.args.qsub_options:
            cmd += " -q '%s'" % (self.args.qsub_options)
        if self.args.run_id:
            cmd += " -r '%s'" % (self.args.run_id)
        if self.args.pipeline_name:
            cmd += " -e '%s'" % (self.args.pipeline_name)
        if self.args.no_prefix:
            cmd += " --no_prefix"
 
        logging.info('running the command: %s' % (cmd))
        proc = sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE, shell=True)
        cmdout, cmderr = proc.communicate()
        if cmdout:
            logging.info(cmdout)
        if cmderr:
            logging.warning(cmderr)
    
    def update_config(self):
        old_config_file = self.args.config_files[0]
        new_config_file = self.args.config_files[1]
        file_name = os.path.join(self.args.working_dir,
                                 self.args.output_filename + '.yaml')
        
        new_config_dict = Configurer.update_config_files(old_config_file, new_config_file)
        Configurer.print2yaml(new_config_dict, file_name)
        
        
    def _make_intermediate_config_file(self, sample_id, sample_dict):
        """make an intermediate config file from the original config_file."""
        intermediate_dir = os.path.join(self.args.working_dir, 'intermediate_config_files')
        make_dir(intermediate_dir)
        temp_name = os.path.splitext(os.path.basename(self.args.config_file))[0] + '_kronos'
        new_file_name = os.path.join(intermediate_dir, sample_id + '_' + temp_name + '.yaml')

        new_config_dict = self.c.update_config_dict(sample_dict)
        Configurer.print2yaml(new_config_dict, new_file_name)
        return new_file_name
        
    def _make_intermediate_pipeline(self, pipeline_name, config_file, sample_id):
        """make an intermediate pipeline script from the intermediate config file."""
        intermediate_dir = os.path.join(self.args.working_dir, 'intermediate_pipeline_scripts')
        make_dir(intermediate_dir)
        
        p = self._make_pipeline(pipeline_name, config_file, intermediate_dir, sample_id)
        self.pipelines.append(p)

    def _make_pipeline(self, pipeline_name, config_file, script_dir, sample_id):
        p = Pipeline(pipeline_name = pipeline_name,
                     config_file   = config_file,
                     script_dir    = script_dir,
                     sample_id     = sample_id)
        p.make_script(sample_id)
        return p
    
    def _paste_pipelines(self, config_file):
        """merge intermediate pipelines."""
        pipeline_script = os.path.join(self.args.working_dir, self.args.pipeline_name + '.py') 
        with open(pipeline_script, 'w') as ps:
            plumber = Plumber(ps, None)
            plumber.paste_pipelines(self.pipelines, config_file)
        
    def test(self):
        pycmd = self.args.python_installation
        tests = list()
        path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(path, '../test/')
        
#       tests.append(os.path.join(path, 'tester_io_manager.py'))
        tests.append(os.path.join(path, 'tester.py'))

        for test in tests:
            os.system('{0} {1}'.format(pycmd, test))

    def make_component(self):
        output_dir = os.path.abspath(self.args.working_dir)
        comp_name = self.args.component_name
        comp_path = os.path.join(output_dir, comp_name)
        
        ## make the component directory
        if os.path.exists(comp_path):
            msg = ("There is already a component with name '{0}' "
                   " in the given path {1}").format(comp_name, comp_path)
            raise Exception(msg)
        else:
            os.mkdir(comp_path)
            os.mkdir(os.path.join(comp_path, 'component_seed'))
        
        def _make_new_file_and_replace_comp_name(template_file, comp_name):
            with open(template_file, 'r') as tf:
                new_filename = os.path.basename(template_file)
                new_file = open(os.path.join(comp_path, new_filename), 'w')
                t = Template(tf.read())
                new_file.write(t.substitute(COMPONENT_NAME=comp_name))
                new_file.close()
                
        package_path = os.path.dirname(os.path.realpath(__file__))
        templates_path = os.path.join(package_path, '../templates')
        component_ui = os.path.join(templates_path, 'component_ui.py')
        component_main = os.path.join(templates_path, 'component_main.py')
        component_reqs = os.path.join(templates_path, 'component_reqs.py')
        component_params =  os.path.join(templates_path, 'component_params.py')
        
        _make_new_file_and_replace_comp_name(component_ui, comp_name)
        _make_new_file_and_replace_comp_name(component_main, comp_name)
        _make_new_file_and_replace_comp_name(component_reqs, comp_name)
        _make_new_file_and_replace_comp_name(component_params, comp_name)
        
        ## create the __init__.py inside the component package
        init_file = open(os.path.join(comp_path, '__init__.py'), 'w')
        init_file.close()
        
def main():
    import kronosui
    args = kronosui.args
    
    logging.info("<<< kronos_" + kronos_version + " started >>>")

    factory = Factory(args)

    if args.subparser_name == 'make_config':
        logging.info("making a config file ...")
        factory.make_config()

    elif args.subparser_name == 'init':
        logging.info("initializing the pipeline ...")
        factory.init_pipeline()

    elif args.subparser_name == 'test':
        factory.test()
        
    elif args.subparser_name == 'make_component':
        logging.info("making a component")
        factory.make_component()
        
    elif args.subparser_name == 'update_config':
        logging.info("updating config files ...")
        factory.update_config()

    elif args.subparser_name == 'run':
        logging.info("running the pipeline ...")
        factory.run_pipeline()
    
    logging.info("<<< kronos_" + kronos_version + " finished >>>")
    
if __name__ == '__main__':
    main()

