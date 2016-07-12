"""
Created on Mar 8, 2014

@author: jtaghiyar
"""
from helpers import validate_argument
import logging

class Plumber(object):

    """
    pipe components into a pipeline based on the given configuration and
    generate a python script.
    """

    def __init__(self, pipeline_script, workflow=None):
        ## the file where the resultant script is written
        self.pipeline_script = pipeline_script
        if workflow is not None:
            self.wf = workflow

    @property
    def wf(self):
        return self._wf
    
    @wf.setter
    def wf(self, value):
        self._wf = value
        self._wf.inflate()
        self.tags = [t for t in self.wf.bfs()]
        self.modes = dict((n.tag,n.use_cluster) for n in self.wf.nodes.values())
        self.mems = dict((n.tag,n.memory) for n in self.wf.nodes.values())
        self.num_cpus = dict((n.tag,n.num_cpus) for n in self.wf.nodes.values())
        
        self.parent_tasks = {}
        self.io_connections = {}
        self.input_arguments = {}
        self.decors = {}
        self.func_strs = {}
        
        self.component_names = [n.component_name for n in self.wf.nodes.values()]
        self.import_components = {k:['component_main as ' + k + '_main']
                                  for k in self.component_names if k != 'breakpoint'}

    def make_script(self, sample_id):
        logging.info('making pipeline for %s ...' % (sample_id))
        
        import_pymod   = {
                          'os' :[],
                          'sys' :[],
                          'ruffus' :[],
                          'traceback' :[],
                          'multiprocessing' :['Queue'],
                          }
        import_factory = {
                          'kronos.pipelineui' :[],
                          'kronos.run_manager' :['RunManager'],
                          'kronos.job_manager' :[
                                                 'DrmaaJobManager',
                                                 'SgeJobManager',
                                                 'LocalJobManager'
                                                 ],
                          'kronos.utils' :['Task'],
                          'kronos.helpers' :[
                                             'JobFailureError',
                                             'flushqueue'
                                             ],
                          'kronos.logger' :[
                                            'PipelineLogger',
                                            'LogWarnErr',
                                            'LogInfo'
                                            ],
                          }
        
        self._get_parent_tasks()
        self._get_io_connections()
        self._get_input_arguments()
        self._get_decorators()
        self._get_function_signatures()

        self._write_importing(import_pymod, comment="import python modules as well as pipelinui")
        self._write_importing(import_factory, comment="import factory modules")
        self._write_initilization()
        self._write_env_preparation()
        self._write_importing(self.import_components, comment="import components")
        self._write_generating_tasks()
        self._write_ruffus_pipeline()
        self._write_last_task()
        self._write_main()

        logging.info('successfully completed.')

    def paste_pipelines(self, pipelines, config_file):
        """paste all the pipelines scripts into a single one."""
        ## store ruffus tasks in a list
        task_funcs = []
        import_dict = {
                       'os' :[],
                       'sys' :[],
                       'ruffus' :[],
                       'subprocess' :[],
                       'datetime' :['datetime'],
                       'multiprocessing' :['Queue'],
                       'kronos.pipelineui' :[],
                       'kronos.helpers' :[
                                          'make_dir',
                                          'kill_jobs',
                                          'flushqueue',
                                          'make_intermediate_cmd_args',
                                          'KeywordsManager'
                                          ],
                       'kronos.logger' :[
                                          'PipelineLogger',
                                          'LogWarnErr',
                                          'LogInfo'
                                          ],
                       'kronos.kronos_version' :['kronos_version']
                       }

        self._write_config_file(config_file)
        self._write_importing(import_dict)
        self._write_argument_validation()        
        self._write_logger()
        
        self._print(comment="ruffus pipeline")
        for i,p in enumerate(pipelines):
            n = p.pipeline_name
            s = p.sample_id
            
            cmd  = '"{python_installation} {4}/{0}.py '
            cmd += '--pipeline_name {0} --working_dir {working_dir} '
            cmd += '--components_dir {components_dir}  --run_id {1} '
            cmd += '--drmaa_library_path {drmaa_library_path} '
            cmd += "--sample_id {2} --qsub_options '{qsub_options}' "
            cmd += '--num_jobs {num_jobs} --log_file {3} '
            cmd += '--extension {extension}" '
            cmd += '.format(pipeline_name, run_id, sample_id, log_file, intermediate_path, **args)'
            
            self._print(message="@ruffus.follows()")
            self._print(message="@LogWarnErr(l)")
            self._print(message="@LogInfo(l)")
            self._print(message="def task_{0}(pipeline_name='{1}'):".format(i,n))
            self._print(message="sample_id = '{}'".format(s), tab=1)
            self._print(message="intermediate_path = os.path.join(os.path.dirname(sys.argv[0]),'intermediate_pipeline_scripts')", tab=1)
            self._print(message="pipeline_script = '{0}/{1}.py'.format(intermediate_path, pipeline_name)", nl=True, tab=1)
            self._print(message="args['pipeline_name'] = pipeline_name", tab=1)
            self._print(message="args['run_id'] = run_id", tab=1)
            self._print(message="args['sample_id'] = sample_id", tab=1)
            self._print(message="args['log_file'] = log_file", nl=True, tab=1)
           
            self._print(message="km = KeywordsManager(pipeline_name, run_id, sample_id, args['working_dir'])", tab=1)
            self._print(message="old_script_content = open(pipeline_script, 'r').read()", tab=1)
            self._print(message="new_script_content = km.replace_keywords(old_script_content)", tab=1)
            self._print(message="f = open(pipeline_script, 'w')", tab=1)
            self._print(message="f.write(new_script_content)", tab=1)
            self._print(message="f.close()", nl=True, tab=1)
                
            self._print(message="cmd = '{0} {1}'.format(args['python_installation'], pipeline_script)", tab=1)
            self._print(message="cmd_args = make_intermediate_cmd_args(args)", tab=1)
            self._print(message="cmd = cmd + ' ' + ' '.join(cmd_args)", tab=1)
            self._print(message="print 'running {} pipeline with command: %s' % (cmd)".format(n), nl=True, tab=1)
            
            self._print(message="proc = subprocess.Popen(cmd, shell=True)", tab=1)
            self._print(message="job_ids.put(proc.pid)", tab=1)
            self._print(message="try:", tab=1)
            self._print(message="cmdout, cmderr = proc.communicate()", tab=2)
            self._print(message="job_rcs.put(proc.returncode)", tab=2)
            self._print(message="except:", tab=1)
            self._print(message="cmd = 'kill %s' % (proc.pid)", tab=2)
            self._print(message="os.system(cmd)", tab=2)
            self._print(message="finally:", tab=1)
            self._print(message="print '{} pipeline finished with exit code %s' % (proc.returncode)".format(n), nl=True, tab=2)
            self._print(message="if cmdout is not None:", tab=1)
            self._print(message="print >> sys.stdout, cmdout", nl=True, tab=2)
            self._print(message="if cmderr is not None:", tab=1)
            self._print(message="print >> sys.stderr, cmderr", nl=True, tab=2)
            task_funcs.append('task_{}'.format(i))

        last = ','.join(task_funcs)
        
        self._print(comment="running pipeline")
        self._print(message="try:")
        self._print(message="ruffus.pipeline_run([{}], multiprocess=args['num_pipelines'])".format(last), tab=1)
        self._print(message="lrc = flushqueue(job_rcs)", tab=1)
        self._print(message="if len(lrc) == len(filter(lambda x: x == 99, lrc)):", tab=1)
        self._print(message="print 'pipeline successfully stopped by breakpoints.'", tab=2)
        self._print(message="rc = 99", tab=2)
        self._print(message="elif not all(rc == 0 for rc in lrc):", tab=1)
        self._print(message="rc = 98", nl=True, tab=2)
        self._print(message="except:")
        self._print(message="rc = -1", tab=1)
        self._print(message="exc_type, exc_obj, exc_tb = sys.exc_info()", tab=1)
        self._print(message="print >> sys.stderr, '{0} pipeline failed due to error: {1}, {2}'.format(pipeline_name, exc_type, exc_obj)", tab=1)
        self._print(message="kill_jobs(job_ids)", nl=True, tab=1)
        self._print(message="finally:")
        self._print(message="pl.log_pipeline_footer(l)", tab=1)
        self._print(message="pl.log_info(l, '{0} pipeline finished with exit code {1}. Please check the logs.'.format(pipeline_name, rc))", tab=1)
        self._print(message="sys.exit(rc)", nl=True, tab=1)
        
        logging.info('successfully pasted pipelines.')


    def _get_parent_tasks(self):
        for t in self.tags:
            ptasks = ["{0}_{1}_function".format(self.wf.nodes[p].component_name, p)
                      for p in self.wf.nodes[t].dependencies]
            ptasks = ", ".join(ptasks)
            self.parent_tasks[t] = ptasks


    def _get_io_connections(self):
        group_ioc = lambda n, p: [ioc for ioc in n.io_connections if ioc.stop_param == p]
        
        for t in self.tags:
            node = self.wf.nodes[t]
            iocs = dict((ioc.stop_param, group_ioc(node, ioc.stop_param))  
                         for ioc in node.io_connections)
            iostr = []
            for k, v_list in iocs.items():
                ## prepend key k (parameter name) with '__pipeline__' to avoid
                ## accidental collision with python reserved keywords
                k = '__pipeline__' + k

                ## v_list is a list of tuples (tag, param)
                ## change it to list of [tag_component.args.param]
                v = [i.start_node + "_component.args." + i.start_param for i in v_list]
                
                if len(v) == 1:
                    ## make sure merge node always has a list as its input.
                    if '_MERGER_' in t:
                        v = '[' + v[0] + ']'
                    else:
                        v = v[0]

                ## change the list to a string like '[1,2,...]'
                else:
                    v = '[' + ', '.join(v) + ']'

                ## paste [k, v] pair together to generate the string 
                ## '__pipeline__k=[__tagname__.component.args.v]'
                iostr.append("=".join([k, v]))
                
            self.io_connections[t] = ", ".join(iostr)


    def _get_input_arguments(self):
        for t in self.tags:
            d = self.wf.nodes[t].input_arguments
            c = self.wf.nodes[t].component_name
            astr = ["=".join(['__pipeline__' + k, repr(validate_argument(v,k,c))])
                    for k,v in d.iteritems() if v != '__OPTIONAL__']
            self.input_arguments[t] = ", ".join(astr)


    def _get_decorators(self):
        for t in self.tags:
            c = self.wf.nodes[t].component_name 
            dp = self.wf.nodes[t].dependencies
            decor  = "@ruffus.follows(*[{0}])\n".format(self.parent_tasks[t])
            if c == 'breakpoint':
                decor += "@ruffus.parallel('{0}', '{1}', {2})\n".format(c, t, dp)
            else:
                decor += "@ruffus.parallel({0}_component.component_name, '{0}', {1})\n".format(t, dp)
            decor += "@ruffus.check_if_uptodate(rm.sentinel_file_exists)\n"
            decor += "@LogWarnErr(l)\n"
            decor += "@LogInfo(l)"
            self.decors[t] = decor

    def _get_exception_handler(self, tag):
        mode = self.modes[tag]
        mem  = self.mems[tag]
        ncpu = self.num_cpus[tag]
        newline = '\n'
        indent = ' ' * 4
        
        if self.wf.nodes[tag].component_name == 'breakpoint':
            expt_str  = ("{ind}rm.generate_sentinel_file(task_name){nl}"
                         "{ind}raise KeyboardInterrupt('breakpoint')"
                         ).format(ind=indent, nl=newline)
            return expt_str
        
        if not mode:
            expt_str  = ("{ind}try:{nl}{ind}{ind}rc = ljm.run_job"
                         "(cmd=run_script, job_name=job_name){nl}"
                         ).format(ind=indent, nl=newline)
        else:
            expt_str  = ("{ind}try:{nl}{ind}{ind}rc = cjm.run_job"
                         "(cmd=run_script, mem='{mem}', ncpus={ncpu}, job_name=job_name)"
                         "{nl}").format(mem=mem, ncpu=ncpu, ind=indent, nl=newline)
       
        expt_str += ("{ind}{ind}job_rcs.put(rc){nl}"
                     "{ind}{ind}if rc == 0:{nl}"
                     "{ind}{ind}{ind}rm.generate_sentinel_file(task_name){nl}"
                    ).format(ind=indent, nl=newline)

        expt_str += ("{ind}except KeyboardInterrupt:{nl}"
                     "{ind}{ind}raise{nl}"
                     "{ind}except:{nl}"
                     "{ind}{ind}job_rcs.put(98){nl}"
                     "{ind}{ind}traceback.print_exc()").format(ind=indent, nl=newline)
        return expt_str

    def _get_function_signatures(self):
        newline = '\n'
        indent = ' ' * 4
        for t in self.tags:
            c = self.wf.nodes[t].component_name
            chunk = self.wf.nodes[t].chunk
            
            bp = self.wf.nodes[t].boilerplate
            ## print it with quotations if string 
            if isinstance(chunk, str):
                chunk = repr(chunk)
            if isinstance(bp, str):
                bp = repr(bp)
            
            func_str  = ("def {0}_{1}_function(*inargs):{nl}"
                        "{ind}component_name, task_name, _ = inargs{nl}"
                        "{ind}print '%s for %s started in %s pipeline' % "
                         "(task_name, component_name, args.pipeline_name)"
                         "{nl}").format(c, t, ind=indent, nl=newline)
                
            if c == 'breakpoint':
                func_str += ("{ind}print 'breakpoint happened in %s' % (task_name)"
                             "{nl}").format(ind=indent, nl=newline)
            else:
                func_str += ("{ind}run_script = rm.generate_script({0}_task, {1}, {2}){nl}"
                             "{ind}job_name = rm.get_filename_prefix(task_name)"
                             "{nl}").format(t, chunk, bp, ind=indent, nl=newline)

            func_str += self._get_exception_handler(t)
            self.func_strs[t] = func_str

    def _print(self, message=None, comment=None, nl=False, tab=None):
        try:
            if tab:
                self.pipeline_script.write(" " * 4 * tab)

            if message:
                self.pipeline_script.write(message + '\n')

            if comment:
                comment_str = "#" + "=" * 80 + "\n#" + comment + "\n#" + "-" * 80
                self.pipeline_script.write(comment_str + '\n')

            if nl:
                self.pipeline_script.write('\n')

        except:
            raise Exception("failed to write to %s" % self.pipeline_script)


    def  _write_importing(self, import_dict, comment='import modules'):
        self._print(comment=comment)

        for k, v in import_dict.iteritems():
            if len(v) == 0:
                self._print(message = "import {0}".format(k))

            else:
                v = ", ".join(v)
                self._print(message = "from {0} import {1}".format(k,v))

        self._print(nl=True)


    def _write_initilization(self):
        self._print(comment="initialization")
        self._print(message="args = kronos.pipelineui.args")
#         self._print(message="sample_id = args.sample_id")
        self._print(message="rm = RunManager(args.run_id, args.pipeline_name, args.working_dir)")

        if not all(self.modes.values()):
            self._print(message="ljm = LocalJobManager(rm.logs_dir, rm.outputs_dir)")

        if any(self.modes.values()):
            self._print(message="if args.job_scheduler.upper() == 'SGE':")
            self._print(message="cjm = SgeJobManager(rm.logs_dir, rm.outputs_dir, args.qsub_options)", tab=1)
            self._print(message="elif args.job_scheduler.upper() == 'DRMAA':")
            self._print(message="try:", tab=1)
            self._print(message="cjm = DrmaaJobManager(args.drmaa_library_path, rm.logs_dir, rm.outputs_dir, args.qsub_options)", tab=2)
            self._print(message="except:", tab=1)
            self._print(message="print >> sys.stderr, 'failed to load DrmaaJobManager'", tab=2)
            self._print(message="traceback.print_exc()", tab=2)
            self._print(message="else:")
            self._print(message="print >> sys.stderr, 'invalid job_scheduler: {}'.format(args.job_scheduler)", tab=1)

        self._print(message="pl = PipelineLogger()")
        self._print(message="l = pl.get_logger(args.pipeline_name, args.log_file)")
        self._print(nl=True)


    def _write_env_preparation(self):
        self._print(comment="environment preparations")
        self._print(message="sys.path.insert(0, args.components_dir)")
        self._print(message="job_rcs = Queue()", nl=True)


    def _write_generating_tasks(self):  
        self._print(comment="generating tasks")
        for t in self.tags:
            c = self.wf.nodes[t].component_name
            if c == 'breakpoint':
                continue
            a = self.input_arguments[t]
            i = self.io_connections[t]
            e = self.wf.nodes[t].env_vars
            
            #update task requirements based on the GENERAL requirements
            reqs = self.wf.nodes[t].requirements
            for k,v in reqs.iteritems():
                if not v:
                    try: 
                        v = self.wf.general_section[k]
                        reqs[k] = v
                    except:
                        pass 

            self._print(message="{0}_component = {1}_main.Component('{1}', component_parent_dir=args.components_dir)".format(t, c))
            self._print(message="{0}_task = Task('{0}', {0}_component)".format(t))
            self._print(message="{0}_task.update_comp_args({1})".format(t, ", ".join([a,i])))
            self._print(message="{0}_prefix = rm.get_filename_prefix('{0}')".format(t))
            self._print(message="{0}_task.update_comp_output_filenames({0}_prefix, rm.outputs_dir, args.no_prefix)".format(t))
            self._print(message="{0}_task.update_comp_env_vars({1})".format(t, e))
#             self._print(message="{0}_task.update_comp_reqs({1})".format(t, self.wf.general_section))
            self._print(message="{0}_task.update_comp_reqs({1})".format(t, reqs))
            self._print(nl=True)


    def _write_ruffus_pipeline(self):
        self._print(comment="ruffus pipeline")

        for t in self.tags:
            self._print(message=self.decors[t])
            s = self.func_strs[t]
            self._print(s)
            self._print(nl=True)


    def _write_last_task(self):
        leaf_nodes_str = ""
        leafs = self.wf.leafs
        for i, l in enumerate(leafs):
            l = self.wf.nodes[l].component_name + '_' + l
            leafs[i] = l 
            leaf_nodes_str += "{%s}_function, " % (i)

        leaf_nodes_str = leaf_nodes_str.format(*leafs)

        message  = "@ruffus.follows(*[{}])\n".format(leaf_nodes_str)
        message += "def __last_task___function():\n"
        message += "    pass"

        self._print(message=message, nl=True)


    def _write_main(self):
        user_colour_scheme = {
                      "colour_scheme_index" :1,
                      "Pipeline"      :{"fontcolor" : '"#FF3232"' },
                      "Key"           :{"fontcolor" : "Red",
                                        "fillcolor" : '"#F6F4F4"' },
                      "Task to run"   :{"linecolor" : '"#0044A0"' },
                      "Final target"  :{"fillcolor" : '"#EFA03B"',
                                        "fontcolor" : "black",
                                        "dashed"    : 0
                                        }
                      }
        printout_cmd  = "ruffus.pipeline_printout_graph({0}, {1}, [__last_task___function], "
        printout_cmd += "draw_vertically = {2}, no_key_legend = {3}, user_colour_scheme = {4})"
        printout_cmd  = printout_cmd.format("args.pipeline_name + '.' + args.extension", 
                                            "args.extension",
                                            "args.draw_vertically",
                                            "args.no_key_legend",
                                            user_colour_scheme)
        
        self._print(comment="main body")
        self._print(message="try:")
        self._print(message="if not args.print_only:", tab=1)
        self._print(message="ruffus.pipeline_run(__last_task___function, multithread=args.num_jobs, verbose=0)", tab=2)
        self._print(message="else:", tab=1)
        self._print(message="cwd = os.getcwd()", tab=2)
        self._print(message="os.chdir(rm.pipeline_dir)", tab=2)
        self._print(message=printout_cmd, tab=2)
        self._print(message="os.chdir(cwd)", tab=2, nl=True)
        self._print(message="lrc = flushqueue(job_rcs)", tab=1)
        self._print(message="if all(rc == 0 for rc in lrc):", tab=1)
        self._print(message="EXIT_CODE = 0", tab=2)
        self._print(message="else:", tab=1)
        self._print(message="EXIT_CODE = 98", tab=2, nl=True)
        self._print(message="except:")
        self._print(message="exc_type, exc_obj, exc_tb = sys.exc_info()", tab=1)
        self._print(message="##exception object is of type <class 'ruffus.ruffus_exceptions.RethrownJobError'>.", tab=1)
        self._print(message="##exc_obj.args[0][3] gives the message in the original exception.", tab=1)
        self._print(message="if exc_obj.args[0][3] == '(breakpoint)':", tab=1)
        self._print(message="print 'breakpoint happened in %s pipeline' % (args.pipeline_name)", tab=2)
        if not all(self.modes.values()):
            self._print(message="ljm.kill_all()", tab=2)
        if any(self.modes.values()):
            self._print(message="try:", tab=2)
            self._print(message="cjm.kill_all()", tab=3)
            self._print(message="except:", tab=2)
            self._print(message="pass", tab=3)
        self._print(message="EXIT_CODE = 99", nl=True, tab=2)
        self._print(message="else:", tab=1)
        self._print(message="print >> sys.stderr, '{0} pipeline failed due to error: {1}, {2}'.format(args.pipeline_name, exc_type, exc_obj)", tab=2)
        if not all(self.modes.values()):
            self._print(message="ljm.kill_all()", tab=2)
        if any(self.modes.values()):
            self._print(message="try:", tab=2)
            self._print(message="cjm.kill_all()", tab=3)
            self._print(message="except:", tab=2)
            self._print(message="pass", tab=3)
        self._print(message="EXIT_CODE = -1", nl=True, tab=2)
        self._print(message="finally:")
        self._print(message="print '{0} pipeline finished with exit code {1}'.format(args.pipeline_name, EXIT_CODE)", tab=1)
        self._print(message="sys.exit(EXIT_CODE)", tab=1)
            

    def _write_logger(self):
        self._print(comment="logger initialization")
        
        self._print(message="pl = PipelineLogger()")
        self._print(message="l = pl.get_logger(pipeline_name, log_file)")
        self._print(message="pl.log_pipeline_header(l, args, pipeline_name, run_id, kronos_version)", nl=True)
        
        self._print(message="args = vars(kronos.pipelineui.args)", nl=True)


    def _write_argument_validation(self):
        self._print(comment="argument validation")
        
        self._print(message="job_ids = Queue()")
        self._print(message="job_rcs = Queue()")
        self._print(message="rc = 0")
        self._print(message="args = kronos.pipelineui.args", nl=True)
        self._print(message="args.components_dir = os.path.abspath(args.components_dir)", nl=True)
        
        self._print(message="if args.pipeline_name is None:")
        self._print(message="pipeline_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]", nl=True, tab=1)
        self._print(message="else:")
        self._print(message="pipeline_name = args.pipeline_name", nl=True, tab=1)
                
        self._print(message="if args.run_id is None:")
        self._print(message="run_id = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')", nl=True, tab=1)
        self._print(message="else:")
        self._print(message="run_id = args.run_id", nl=True, tab=1)

        self._print(message="make_dir(args.working_dir)")
        self._print(message="args.working_dir = os.path.join(args.working_dir, run_id)")
        self._print(message="make_dir(args.working_dir)", nl=True)
        self._print(message="if args.log_file is None:")
        self._print(message="log_file = os.path.join(args.working_dir, '_'.join([pipeline_name, run_id]) + '.log')", nl=True, tab=1)
        self._print(message="else:")
        self._print(message="log_file = os.path.join(args.working_dir, '_'.join([args.log_file, run_id]) + '.log')", nl=True, tab=1)
        
        self._print(comment="make a copy of the config file")
        self._print(message="cfile = os.path.join(args.working_dir, pipeline_name + '_' + run_id + '.yaml')")
        self._print(message="with open(cfile, 'w') as cf:")
        self._print(message="cf.write(config_file_content)", nl=True, tab=1)
        
#         self._print(comment="limit the maximum number of simultaneous jobs")
#         self._print(message="max_num_total = 500")
#         self._print(message="max_num_pips = 10")
#         self._print(message="max_num_jobs = int(max_num_total / max_num_pips)")
# #         self._print(message="numok = lambda: args.num_pipelines * args.num_jobs <= max_num_total", nl=True)
# #         self._print(message="if not numok():")
# #         self._print(message="if args.num_pipelines > max_num_pips:", tab=1)
# #         self._print(message="args.num_pipelines = max_num_pips", tab=2)
# #         self._print(message="if not numok():", tab=2)
# #         self._print(message="args.num_jobs = max_num_jobs", tab=3)
# #         self._print(message="else:", tab=1)
# #         self._print(message="args.num_jobs = max_num_jobs", tab=2)
#         self._print(message="if args.num_pipelines > max_num_pips:")
#         self._print(message="args.num_pipelines = max_num_pips", tab=1)
#         self._print(message="if args.num_jobs > max_num_jobs:")
#         self._print(message="args.num_jobs = max_num_jobs", tab=1)
        
    def _write_config_file(self, config_file):
        """ print the content of the config file."""
        with open(config_file, 'r') as cfile:
            self._print(comment="config file content")
            self._print(message="config_file_content = '''")
            lines = cfile.readlines()
            for l in lines:
                self._print(message=l.strip('\n'))
                
            self._print(message="'''")
            self._print(nl=True)










