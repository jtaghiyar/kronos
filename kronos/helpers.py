"""
Created on Jun 2, 2014

@author: jtaghiyar, jrosner
"""

import os
import sys
import warnings
import yaml
import getpass
from datetime import datetime
from ast import literal_eval
from collections import OrderedDict, defaultdict
from string import Template
from kronos_version import kronos_version 

class JobIdNotFound(Exception):

    """
    exception for not founding the job_id.
    """

    def __init__(self, job_id):
        self.job_id = job_id
        self.err_msg = 'job id not found: %s' % (job_id)

class JobFailureError(Exception):

    """
    exception for job run-time errors.
    """

    def __init__(self, job_id, exit_code, err_msg=None):
        self.job_id = job_id
        self.err_msg = err_msg
        self.exit_code = exit_code

class ConfigError(Exception):

    """
    exception for configuration file errors.
    """

    pass


class Tree(object):

    """
    tree-like data structure.
    """

    def __init__(self):
        self._items = defaultdict(Tree)

    def __getitem__(self, key):
        return self.items[key]

    def __setitem__(self, key, value):
        self.items[key] = value

    def __delitem__(self, key):
        del self._items[key]
    
    def __eq__(self, other):
        return self.todict() == other.todict()

    def __repr__(self):
        return repr(self.todict())
    
    def __len__(self):
        return len(self.nodes) + len(self.leafs)

    @property 
    def items(self):
        return self._items

    @property
    def leafs(self):
        """return leafs of the tree. A leaf is a (key, value) pair."""
        def _leafs(t=None, leafs=None):
            if t is None:
                t = self
    
            if leafs is None:
                leafs = list()
    
            for k, v in t.items.iteritems():
                if isinstance(v, Tree):
                    _leafs(v, leafs)
                else:
                    leafs.append((k, v))
    
            return leafs
        
        return _leafs()

    @property
    def nodes(self):
        """return nodes of the tree that are NOT leafs."""
        def _nodes(t=None, l=None):
            if t is None:
                t = self
            
            if l is None:
                l = list()
                
            for k, v in t.items.iteritems():
                if isinstance(v, Tree):
                    l.append(k)
                    _nodes(v, l)
            
            return l

        return _nodes()

    @property
    def paths(self):
        """return all the paths in the tree."""

        def _paths(d):
            paths = list()
    
            for k, v in d.iteritems():
                if isinstance(v, Tree):
                    v = v.todict()
    
                if isinstance(v, dict):
                    res = _paths(v)
                    for path in res:
                        paths.append({k:path})
                else:
                    paths.append({k:v})
    
            return map(self.dict2tree, paths)

        return _paths(self.items)

    def todict(self):
        """transform a tree to a nested dictionary."""
        def _todict(t=None, d=None):
            if t is None:
                t = self
    
            if d is None:
                d = dict()
    
            for k, v in t.items.iteritems():
                if isinstance(v, Tree):
                    d[k] = dict()
                    _todict(v, d[k])
                else:
                    d[k] = v
            else:
                return d

        return _todict()

    def add_path(self, path):
        """add path to the tree."""
        def h(t, k):
            if isinstance(t, Tree) and isinstance(t[k], Tree):
                return t[k]
            else:
                t[k] = Tree()
                return t[k]

        keys = self.get_path_keys(path)
        t = reduce(h, keys[:-1], self)
        t[keys[-1]] = path.leafs[0][1]

    def update(self, tree):
        """update self with given tree."""
        for path in tree.paths:
            self.add_path(path)

    def get_from_path(self, path):
        return reduce(lambda t,k: t[k], path.nodes, self)

    def empty(self):
        if len(self):
            return False
        return True
    
    @staticmethod
    def dict2tree(d):
        """transform a nested dictionary to a tree of type Tree."""
        def _dict2tree(d, t=None):
            if t is None:
                t = Tree()

            for k,v in d.iteritems():
                if isinstance(v, dict):
                    _dict2tree(v, t[k])
                else:
                    t[k] = v
            else:
                return t

        return _dict2tree(d, None)

    @staticmethod
    def get_path_keys(path):
        keys = list()
        path = path.todict()
        while True:
            k, v = path.items()[0]
            keys.append(k)
            if not isinstance(v, dict):
                break
            path = v
        return keys

    @staticmethod
    def update_leaf(path, value):
        keys = Tree.get_path_keys(path)
        reduce(lambda d, k: d[k], keys[:-1], path)[keys[-1]] = value
        return path


class OrderedTree(Tree):
    
    """
    preserve the order in which nodes are added.
    """
    
    def __init__(self):
        self._items = OrderedDict()

    def __getitem__(self, key):
        try:
            return self.items[key]
        except KeyError:
            return self.__missing__(key)
        
    def __missing__(self, key):
        self[key] = value = OrderedTree()
        return value

    def __setitem__(self, key, value):
        self.items[key] = value

    def todict(self):
        """transform an ordered tree to a nested ordered dictionary."""
        def _todict(t=None, d=None):
            if t is None:
                t = self
            
            if d is None:
                d = OrderedDict()
            
            for k, v in t.items.iteritems():
                if isinstance(v, OrderedTree):
                    d[k] = OrderedDict()
                    _todict(v, d[k])
                else:
                    d[k] = v
            else:
                return d

        return _todict()


class ComponentParser(object):
    
    """
    parse component_reqs.py and component_params.py of the given component.
    """
    
    def __init__(self, component_name):
        self.component_name = component_name
        self.component_version = None
        self.env_vars = {}
        self.input_files = {}
        self.input_params = {}
        self.memory = None
        self.num_cpus = 1
        self.output_files = {}
        self.parallel = False
        self.requirements = {}
        self.return_value = None
        self.seed_version = None

    def parse_params(self):
        """parse the component_params.py module of the given component."""
        list_of_mods = ['component_params']
        temp_modules = __import__(self.component_name, globals(), locals(), list_of_mods, -1)

        self.input_files  = temp_modules.component_params.input_files
        self.input_params = temp_modules.component_params.input_params
        self.output_files = temp_modules.component_params.output_files
        self.return_value = temp_modules.component_params.return_value

    def parse_reqs(self):
        """parse the component_reqs.py module of the given component."""
        list_of_mods = ['component_reqs']
        temp_modules = __import__(self.component_name, globals(), locals(), list_of_mods, -1)
        
        self.component_version = temp_modules.component_reqs.version
        self.env_vars          = temp_modules.component_reqs.env_vars
        self.memory            = temp_modules.component_reqs.memory
        self.parallel          = temp_modules.component_reqs.parallel
        self.requirements      = temp_modules.component_reqs.requirements
        self.seed_version      = temp_modules.component_reqs.seed_version
        
 
class Configurer(object):
    
    """
    Make/update a yaml config file.
    """
    
    def __init__(self, config_file=None):
        self._config_dict = {}
        self.config_file = config_file

    @property
    def config_dict(self):
        if self._config_dict:
            return self._config_dict 
        elif self.config_file:
            return self.read_config_file(self.config_file)
        else:
            return {}

    @config_dict.setter
    def config_dict(self, value):
        self._config_dict = value
 
    def update_config_dict(self, new_config_dict):
        """update the config dict with the given new config dict."""
        temp_tree = Tree.dict2tree(self.config_dict)
        temp_tree.update(Tree.dict2tree(new_config_dict))
        return temp_tree.todict()

    def update_sample_section(self, samfile):
        """update __SAMPLES__ section using given sample file.
        
        Parse samfile into a tree as:
        '__SAMPLES__':
        { 
            id1:{col1:val11, col2:val21, ...},
            id2:{col1:val21, col2:val22, ...},
            ...
        }
        and update self.cofig_dict using this new tree.
        """

        t = Tree()        
        with open(samfile, 'r') as infile:
            header = infile.readline().strip().split('\t')
            if header[0] != "#sample_id":
                errmsg = "the column name of the first column in input samples"
                errmsg += " file should be '#sample_id'"
                raise Exception(errmsg)

            for l in infile:            
                l = l.strip().split('\t')
                if len(l) != len(header):
                    raise Exception("too many/few items in line %s" % l)
                
                for k,v in zip(header[1:], l[1:]):
                    v = evaluate_variable(v)
                    t.add_path(t.dict2tree({l[0]:{k:v}}))

            d = {'__SAMPLES__': t.todict()}
            self.config_dict = self.update_config_dict(d)                        

    def update_shared_section(self, setupfile):
        """update sections using given setup file.
        
        Parse setupfile into a tree as:
        '__SHARED__':
        {
            param1: value1,
            param2: value2,
            ...
        }
        and update the self.config_dict using this new tree.
        """
        
        t = Tree()
        with open(setupfile, 'r') as infile:
            header = infile.readline().strip().split('\t')
            if header[0] != "#section":
                errmsg = "the column name of the first column in input setup"
                errmsg += " file should be '#section'"
                raise Exception(errmsg)
            
            for l in infile:
                l = l.strip().split('\t')
                if len(l) != len(header):
                    raise Exception("too many/few items in line %s" % l)
                
                ## this is for backward compatibility with existing setup files 
#                 if l[0] in ('__OPTIONS__'):
#                     continue
                
                v = evaluate_variable(l[2])
                if l[0] in ('__SHARED__', '__GENERAL__'):
                    path = {l[0]:{l[1]:v}}
                else:
                    path = {l[0]:{'run':{'requirements':{l[1]:v}}}}
                t.add_path(t.dict2tree(path))
            
            d = t.todict()
            self.config_dict = self.update_config_dict(d)
    
    @staticmethod
    def make_config_dict(component_names):
        """make a config dict for given list of components."""
        id_number = 0
        config_dict = OrderedDict()
        config_dict['__PIPELINE_INFO__'] = Configurer._get_info_section_config_dict()
        config_dict['__GENERAL__'] = Configurer._get_general_section_config_dict(component_names)
        config_dict['__SHARED__'] = {}
        config_dict['__SAMPLES__'] = {}
        for component_name in component_names:
            id_number += 1
            tag = "__TASK_{}__".format(id_number)
            config_dict[tag] = Configurer._get_task_section_config_dict(component_name)
        
        return config_dict

    @staticmethod
    def print2yaml(config_dict, file_name):
        """print the config dict to a yaml file."""
        config_dict = Configurer._sort_config_dict(config_dict)
        Configurer._print_helper(config_dict, open(file_name, 'w'))
        
    @staticmethod
    def update_config_files(old_config_file, new_config_file):
        """copy the fields of old_config_file to new_config_file."""
        old_config_dict = yaml.load(open(old_config_file))
        new_config_dict = yaml.load(open(new_config_file))
        
        old_tree = Tree.dict2tree(old_config_dict)
        new_tree = Tree.dict2tree(new_config_dict)
        
        ## preserve the factory version of the new config file
        kv =  new_tree['__PIPELINE_INFO__']['kronos_version']
        new_tree.update(old_tree)
        new_tree['__PIPELINE_INFO__']['kronos_version'] = kv
        
        return new_tree.todict()

    @staticmethod
    def read_config_file(config_file):
        """read yaml config file and return corresponding config dict."""
        config_dict = yaml.load(open(config_file, 'r'))
        if not config_dict:
            raise ConfigError(config_file +' Config file is empty')
        
        return config_dict
               
    @staticmethod 
    def _get_info_section_config_dict():
        """create the config dict for PIPELINE_INFO section."""
        section_tree = OrderedTree()
        section_tree['name'] = None
        section_tree['version'] = "0.99.0"
        section_tree['author'] = getpass.getuser()
        section_tree['data_type'] = None
        section_tree['input_type'] = None
        section_tree['output_type'] = None
        section_tree['host_cluster'] = None
        section_tree['date_created'] = datetime.now().strftime("%Y-%m-%d")
        section_tree['date_last_updated'] = None
        section_tree['Kronos_version'] = kronos_version
        return section_tree.todict()
    
    @staticmethod
    def _get_general_section_config_dict(component_names):
        """create the config dict for GENERAL section from the component_reqs."""
        all_requirements = {}
        for component_name in component_names:
            cparser = ComponentParser(component_name)
            cparser.parse_reqs()
            all_requirements.update(cparser.requirements)
        return all_requirements
       
    @staticmethod
    def _get_task_section_config_dict(component_name):
        """create config dict for TASK sections."""
        cparser = ComponentParser(component_name)
        cparser.parse_params()
        cparser.parse_reqs()
        
        section_tree = OrderedTree()
        section_tree['reserved']['component_name'] = component_name
        section_tree['reserved']['component_version'] = cparser.component_version
        section_tree['reserved']['seed_version'] = cparser.seed_version
        section_tree['run']['use_cluster'] = False
        section_tree['run']['memory'] = cparser.memory
        section_tree['run']['num_cpus'] = cparser.num_cpus 
        section_tree['run']['forced_dependencies'] = []
        section_tree['run']['add_breakpoint'] = False
        section_tree['run']['env_vars'] = cparser.env_vars
        section_tree['run']['boilerplate'] = None
        section_tree['component']['input_files'] = cparser.input_files
        section_tree['component']['output_files'] = cparser.output_files
        section_tree['component']['parameters'] = cparser.input_params
        
        for k, v in cparser.requirements.iteritems():
            section_tree['run']['requirements'][k] = v
             
        if cparser.parallel:
            section_tree['run']['parallel_run'] = False
            section_tree['run']['parallel_params'] = []
            section_tree['run']['interval_file'] = None
            
        return section_tree.todict()  
    
    @staticmethod
    def _check_yaml_boolean(config_dict):
        """check for automatic boolean casting by yaml.load() method."""        
        YAML_BOOLEANS = (['y', 'Y', 'yes', 'Yes', 'YES', 'n', 'N', 'no', 'No', 'NO',
                          'true', 'True', 'TRUE', 'false', 'False', 'FALSE',
                          'on', 'On', 'ON', 'off', 'Off', 'OFF'])
    
        for k, v in config_dict.iteritems():
            if isinstance(v, dict):
                Configurer._check_yaml_boolean(v)
    
            ## NOTE: Correction is not necessary for arguments because
            ##       all string arguments are wrapped in quotes in the
            ##       print_yaml_helper function.
            if k in YAML_BOOLEANS:
                config_dict[repr(k)] = config_dict.pop(k)   
        return config_dict         

    @staticmethod
    def _print_helper(config_dict, f_stream, indent=0):
        """recursively print a dictionary to yaml format."""
        def _get_samples_section_example():
            """print an example sample entry to the SAMPLES section."""
            newline = '\n'
            indent = ' ' * 4
            comment = ("{nl}"
                       "{i}# sample_id:{nl}"
                       "{i}#{i}param1: value1{nl}"
                       "{i}#{i}param2: value2{nl}").format(i=indent, nl=newline)
            
            return comment
        
        # iterate over top-level of dictionary
        config_dict = Configurer._check_yaml_boolean(config_dict)
        for key, value in config_dict.iteritems():
            yaml_line = ' ' * 4 * indent + str(key) + ': '
    
            # add a comment or an example to the '__SAMPLES__' section
            if key == '__SAMPLES__':
                yaml_line += _get_samples_section_example()
    
            # add a comment to reserved section
            if key == 'reserved':
                yaml_line += '\n' + ' ' * 4 * (indent + 1) 
                yaml_line += '# do not change this section.'
    
            # add comment to run section
            elif key == 'run' and not isinstance(value.get('parallel_run'), bool):
                yaml_line += '\n' + ' ' * 4 * (indent + 1) 
                yaml_line += '# NOTE: component cannot run in parallel mode.'

            f_stream.write(yaml_line)
                
            if isinstance(value, dict):
                f_stream.write('\n')
                Configurer._print_helper(value, f_stream, indent + 1)
            elif value is None:
                f_stream.write('\n')
            else:
                f_stream.write(repr(value) + '\n')

    @staticmethod
    def _sort_config_dict(config_dict):
        """sort the sections in the config dict"""
        ## The order that special sections appear in the config file should be preserved.
        ## Also they should appear in the beginning of the config file. 
        special_sections = ['__PIPELINE_INFO__', '__GENERAL__', '__SHARED__', '__SAMPLES__']
        sorted_config_dict = OrderedDict().fromkeys(special_sections)
        sorted_config_dict.update(sorted(config_dict.items()))
        return sorted_config_dict


class KeywordsManager(object):
    
    """detect and replace keywords in the config file."""
    
    def __init__(self, pipeline_name, run_id, sample_id, pipwd):
        self.keywords = {'pipeline_name': pipeline_name,
                         'run_id': run_id,
                         'sample_id': sample_id,
#                          'current_working_dir': os.getcwd(),
                         'pipeline_working_dir': pipwd
                         }
        
    def replace_keywords(self, stream):
        """replace keywords in the config file."""
        t = Template(stream)
        res = None
        try:
            res = t.substitute(**self.keywords)
        except KeyError as e:
            print >> sys.stderr, 'unrecognized keyword in the config file: %s' % ('$' + e.message)
        return res


def make_intermediate_cmd_args(args_dict):
    cmd_args = list()
    for k, v in args_dict.iteritems():
        k = "--" + k
        if isinstance(v, bool) and v:
            cmd_args.append(k)
        elif v is not None and not isinstance(v, bool):
            if isinstance(v, str):
                v = repr(v)
            cmd_args.append("{0} {1}".format(k, v))
    return cmd_args


def evaluate_variable(eval_var):
    """perform a literal_eval of a variable."""
    try:
        return literal_eval(eval_var)

    except (ValueError, SyntaxError):
        return eval_var


def validate_argument(arg, param, component_name):
    """validate config file arguments."""
    # check for missing required arguments
    if arg == '__REQUIRED__':
        msg = "parameter '{0}' is required in component '{1}'".format(param, component_name)
        raise ConfigError(msg)
 
    # skip over unspecified optional parameters
#     elif arg == '__OPTIONAL__' or arg == 'None':
    elif arg == 'None':
        arg = None
 
    elif arg == '__FLAG__':
        arg = False
 
    return arg


def make_dir(dirname):
    """make a directory if not exists."""
    if not os.path.exists(dirname):
        os.mkdir(dirname)


def trim(d, prepended_msg):
    """remove the prepended-msg from the keys of dictionary d."""
    keys = [x.split(prepended_msg)[1] for x in d.keys()]
    return {k:v for k,v in zip(keys, d.values())}


def kill_jobs(q):
    """kill all the jobs in the queue."""
    while not q.empty():
        job_id = q.get()
        cmd = "kill %s" % (job_id)
        os.system(cmd)


def flushqueue(q):
    """copy a queue into a list."""
    l = list()
    while not q.empty():
        elem = q.get()
        l.append(elem)
    return l


def export_to_environ(path, envar):
    """ export path to envar environment variable."""
    if os.environ.get(envar):
        if path not in os.environ[envar].split(':'):
            os.environ[envar] = path + ':' + os.environ[envar]

    else:
        os.environ[envar] = path


# class PrintPipeline(object):
#     
#     """
#     wrap ruffus.pipeline_printout_graph to make component_names and the pipeline name
#     appear to the printed document at the end.
#     """
#     
#     def __init__(self, task_to_comp_map):
#         from ruffus import pipeline_printout_graph as ppg
#         self._ppg = ppg
#         self._map = task_to_comp_map
# 
#     def _make_ruffus_pipeline(self):
#         
#         pass
#     
#     def printout_pipeline(self, filename, extention, draw_vertically=False, no_key_legend=True):
#         self.ppg(filename, extention, self._task_names,
#                  draw_vertically = draw_vertically,
#                  no_key_legend = no_key_legend,
#                  user_colour_scheme = {
#                                        "colour_scheme_index" :1,
#                                        "Pipeline"      :{"fontcolor" : '"#FF3232"' },
#                                        "Key"           :{"fontcolor" : "Red",
#                                                          "fillcolor" : '"#F6F4F4"' },
#                                        "Task to run"   :{"linecolor" : '"#0044A0"' },
#                                        "Final target"  :{"fillcolor" : '"#EFA03B"',
#                                                           "fontcolor" : "black",
#                                                           "dashed"    : 0           }
#                                        })

