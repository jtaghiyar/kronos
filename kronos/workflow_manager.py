"""
Created on Sep 9, 2014

@author: jtaghiyar
"""

import sys
from helpers import *
from warnings import warn

class Connection(object):
    
    """
    connection between different attributes of different sections.
    """
    
    def __init__(self, start_node, start_param, stop_node, stop_param, path):
        self.start_node = start_node
        self.start_param = start_param
        self.stop_node = stop_node
        self.stop_param = stop_param
        self.path = path
        self.value = None
    
    def __repr__(self):
        return "({start_node},{start_param}) --> ({stop_node},{stop_param})".format(**self.__dict__)
    
    def get_value(self):
        return self.value
    

class IOConnection(object):

    """
    io-connection, i.e. directed edge from/to a node in a workflow.
    """
    
    def __init__(self, start_node, start_param, stop_node, stop_param):
        self.start_node = start_node
        self.start_param = start_param 
        self.stop_node = stop_node
        self.stop_param = stop_param
        
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "({start_node},{start_param}) --> ({stop_node},{stop_param})".format(**self.__dict__)

    def __iter__(self):
        yield self.__dict__.values()
        
    def copy(self):
        return IOConnection(**self.__dict__)
        

class WorkFlowNode(object):
    """
    WorkFlowNode class represents a node in a WorkFlow object,
    which is equivalent to a section in a factory config file.
    """
    
    def __init__(self, config_dict):
        self._config_dict = config_dict
        self._ps = Tree.dict2tree(config_dict)
        self._fd = None
        self._iocs = None
        self.tag = None
        self.chunk = None
        self.parent = None
        self.children = []
        self.parallelized = False
        
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @property
    def properties(self):
        return self._ps
    
    @property
    def config_dict(self):
        return self._config_dict
    
    @property
    def component_name(self):
        res = self.properties['reserved']['component_name']
        return res if isinstance(res, str) else None

    @property
    def component_version(self):
        res = self.properties['reserved']['component_version']
        return res if not isinstance(res, Tree) else None
    
    @property
    def seed_version(self):
        res = self.properties['reserved']['seed_version']
        return res if not isinstance(res, Tree) else None
    
    @property
    def input_arguments(self):
        leafs = self.properties['component'].leafs
        return {leaf[0]:leaf[1] for leaf in leafs if not self._isconnection(leaf[0])}

    @property
    def input_files(self):
        res = self.properties['component']['input_files']
        return res

    @property
    def output_files(self):
        res = self.properties['component']['output_files']
        return res
            
    @property
    def use_cluster(self):
        res = self.properties['run']['use_cluster']
        return True if isinstance(res, bool) and res else False 

    @property
    def memory(self):
        res = self.properties['run']['memory']
        return res if not isinstance(res, Tree) else None

    @property
    def num_cpus(self):
        res = self.properties['run']['num_cpus']
        return int(res) if not isinstance(res, Tree) else 1

    @property
    def parallel_run(self):
        res = self.properties['run']['parallel_run']
        return True if isinstance(res, bool) and res else False 

    @property
    def parallel_params(self):
        res = self.properties['run']['parallel_params']
        return res if not isinstance(res, Tree) else []

    @property
    def interval_file(self):
        res = self.properties['run']['interval_file']
        return res if not isinstance(res, Tree) else None
        
    @property
    def env_vars(self):
        res = self.properties['run']['env_vars']
        return res.todict() if isinstance(res, Tree) else {}
    
    @property
    def requirements(self):
        res = self.properties['run']['requirements']
        return res.todict() if isinstance(res, Tree) else {}
    
    @property
    def boilerplate(self):
        res = self.properties['run']['boilerplate']
        return res if not isinstance(res, Tree) else None

    @property
    def breakpoint(self):
        res = self.properties['run']['add_breakpoint']
        return True if isinstance(res, bool) and res else False
    
    @property
    def chunks(self):
        if not self.parallel_run:
            chunks = []
        elif self.interval_file is not None and self.interval_file != 'None':
            lines = open(self.interval_file, 'r').readlines()
            chunks = [l.strip() for l in lines]
        else:
            chunks = map(str, range(1,23)) + ['X','Y']
        return chunks

    @property
    def forced_dependencies(self):
        if self._fd is not None:
            return self._fd
        res = self.properties['run']['forced_dependencies']
        return res if not isinstance(res, Tree) else []

    @forced_dependencies.setter
    def forced_dependencies(self, value):
        self._fd = value
   
    @property
    def dependencies(self):
        deps = [ioc.start_node for ioc in self.io_connections]
        deps.extend(self.forced_dependencies)
        return sorted(deps)

    @property
    def sample_connections(self):
        scs = [IOConnection('__SAMPLES__', param, self.tag, arg)
               for arg, tag, param, _ in self._iterate_tuple_leafs()
               if '__SAMPLES__' in tag]
        return sorted(scs)
#     
#     @property
#     def sample_connection_paths(self):
#         args = [sc.stop_param for sc in self.sample_connections]
#         return [p for p in self.properties.paths if p.leafs[0][0] in args]

    @property
    def io_connections(self):
        if self._iocs is not None:
            return sorted(self._iocs)
        iocs = [IOConnection(tag, param, self.tag, arg)
                for arg, tag, param, _ in self._iterate_tuple_leafs()
                if '__TASK_' in tag]
        return sorted(iocs)
    
    @io_connections.setter
    def io_connections(self, value):
        if not isinstance(value, list):
            warn('value must be a list, "io_connections" is not set.')
            return
        if not all(isinstance(v, IOConnection) for v in value):
            warn('none "IOConnection" entry in the list, "io_connections" is not set.')
            return
        self._iocs = value

    @property
    def connections(self):
        return [Connection(tag, param, self.tag, arg, path)
                for arg, tag, param, path in self._iterate_tuple_leafs()]
    
    def copy(self):
        """make a copy."""
        other = WorkFlowNode(self.config_dict)
        if self._iocs is not None:
            other.io_connections = self._iocs[:]
        if self._fd is not None:
            other.forced_dependencies = self._fd[:]
        return other

    def issyncable(self, other, param):
        """decide if node can be synchronized with another node."""
        if not self.parallel_run or not other.parallel_run:
            return False
        
        if self.parallelized and not other.parallelized:
            return False
        
        if not param in self.parallel_params:
            return False
        
        if not any(ioc.start_node == other.tag and ioc.stop_param == param 
                   for ioc in self.io_connections):
            return False

        # interval file takes precedence over the synchronization.
        # if not self.interval_file and other.interval_file:
        if self.interval_file:
            return False
        
        if self.parallelized and not len(self.children) == len(other.children):
            return False
        
        return True

    def expand(self):
        """spawn children nodes."""
        if not self.parallel_run or self.parallelized:
            return 
        for i, chunk in enumerate(self.chunks):
            newn = self.copy()
            newn.tag = self.tag + '_%s_' % (i + 1)
            newn.parent = self
            newn.chunk = chunk
            newn.forced_dependencies = self.forced_dependencies
            self.children.append(newn)
        self.parallelized = True
    
    def compress(self):
        """remove children."""
        self.children = []
        self.parallelized = False

    def _isconnection(self, arg):
        for ioc in self.io_connections:
            if arg == ioc.stop_param:
                return True
        for sc in self.sample_connections:
            if arg == sc.stop_param:
                return True
        return False

    def _iterate_tuple_leafs(self):
        """iterate over all the leafs and return it if is tuple. Also check for tuple of tuples."""
        for path in self.properties.paths:
            leaf = path.leafs[0]
            arg = leaf[0]
            value = evaluate_variable(leaf[1])
            if isinstance(value, tuple):
                for v in value:
                    if not isinstance(v, tuple):
                        break
                    else:
                        tag, param = v
                        yield arg, tag, param, path
                else:
                    continue
                tag, param = value 
                yield arg, tag, param, path


class BreakPoint(WorkFlowNode):
    
    """
    a break-point node, special node to cause pipeline to stop with a special return code.
    """
    
    def __init__(self, name, predecessor_tag):
        t = Tree()
        t['reserved']['component_name'] = 'breakpoint'
        t['run']['forced_dependencies'] = [predecessor_tag] 
        super(BreakPoint, self).__init__(t.todict())
        self.tag = name


class Merger(WorkFlowNode):

    """
    make a merger node, update the io_connections accordingly and run the 'merge' component.
    """
    
    def __init__(self, node, p, ioc):
        t = Tree()
        tag = '_'.join([p.tag, ioc.start_param, 'MERGER', node.tag, ioc.stop_param])
        t['reserved']['component_name'] = 'merge'
        t['run']['use_cluster'] = p.use_cluster
        t['run']['memory'] = '10G'
        t['run']['interval_file'] = None
        t['run']['parallel_run'] = False
        t['run']['parallel_params'] = []
        t['run']['env_vars'] = None
        t['reserved']['component_version'] = '0.99.0'
        t['run']['forced_dependencies'] = []
        t['component']['input_files'] = {'infiles': None}
        t['component']['output_files'] = {'out': 'merge/' + tag + '.merged'}
        t['component']['input_params'] = {'extension': None}

        super(Merger, self).__init__(t.todict())
        self._node = node
        self._ioc = ioc  
        self.tag = tag      
        self.io_connections = [IOConnection(child.tag, ioc.start_param, self.tag, 'infiles')
                               for child in p.children]

    def update_io_connections(self):
        """add the merger node to the io_connections of the node"""
        startn = self._ioc.start_node
        startp = self._ioc.start_param
        stopp  = self._ioc.stop_param

        ## if node.parallelized, only update its children's io_connections
        if self._node.parallelized:
            children = []
            for child in self._node.children:
                iocs = self._update_ioc(child.io_connections, startn, startp, stopp)
                child.io_connections = iocs
                children.append(child)
            self._node.children = children
        else: 
            self._node.io_connections = self._update_ioc(self._node.io_connections, startn, startp, stopp)
        
        return self._node

    def _update_ioc(self, io_connections, start_node, start_param, stop_param):
        iocs = []
        for ioc in io_connections:
                if ioc.start_node == start_node and ioc.start_param == start_param\
                and ioc.stop_param == stop_param:
                    ioc.start_node = self.tag
                    ioc.start_param = 'out'
                iocs.append(ioc)
        return iocs            

class Paralleler(object):
 
    """
    parallelize/synchronize a node.
    """
    
    def expand(self, node, number_of_children):
        """spawn children nodes."""
        for i in range(number_of_children):
            newn = node.copy()
            newn.tag = node.tag + '%s_' % i
            newn.parent = node
            newn.chunk = node.chunks[i]
            newn.forced_dependencies = node.forced_dependencies
            node.children.append(newn)
        node.parallelized = True  
        return node
    
    def synchronize(self, node, p, param):
        """
        expand node as many as the children of its predecessor node p,
        if not already parallelized, and update its children's io_connections.
        """
        if not node.parallelized:
            node = self.expand(node, len(p.children))
        self._update_children_io_connections(node, p, param)
        node.parallelized = True       
        return node

    def _update_children_io_connections(self, node, p, param):
        """update the io_connections of node's children from p via param"""
        for i, child in enumerate(node.children):
            iocs = []
            for ioc in child.io_connections:
                if ioc.stop_node == node.tag:
                    ioc.stop_node = child.tag
                if ioc.start_node == p.tag and ioc.stop_param == param:
                    ioc.start_node = p.children[i].tag
                iocs.append(ioc)
            child.io_connections = iocs
            

class WorkFlow(object):

    """
    make a workflow, i.e. an acyclic directed graph, from the given 
    :param config_dictd: a (nested) dictionary (from loading yaml configuration file) 
    """
    
    def __init__(self, config_file):
        self._nodes = dict()
        self._breakpoints = dict()
        self._folded_nodes = dict()
        self._info_section = dict()
        self._general_section = dict()
        self._shared_section = dict()
        self._samples_section = dict()
        self._config_file = config_file
        self._config_dict = Configurer.read_config_file(config_file)
        self.inflated = False
        self._populate()
        self._apply_breakpoints()
        
    @property
    def config_file(self):
        return self._config_file
    
    @property
    def config_dict(self):
        return self._config_dict
    
    @property
    def info_section(self):
        return self._info_section

    @property
    def general_section(self):
        return self._general_section

    @property
    def shared_section(self):
        return self._shared_section

    @property
    def samples_section(self):
        return self._samples_section

    @property
    def nodes(self):
        return self._nodes
    
    @property
    def edges(self):
        return [ioc for n in self.nodes.values() for ioc in n.io_connections]

    @property
    def roots(self):
        return [t for t in self.nodes.keys() if not len(self.nodes[t].dependencies)]

    @property
    def leafs(self):
        tags = self.nodes.keys()
        all_deps = reduce(lambda x,y: x + y, [n.dependencies for n in self.nodes.values()])
        leafs = set(tags).difference(set(all_deps))
        return list(leafs)
    
    def add_node(self, tag, node):
        """add a new node."""
        self._nodes[tag] = node
        self._nodes[tag].tag = tag

    def isdependent(self, tag1, tag2):
        """check if the node with tag1 depends on the node with tag2."""
        node1 = self.nodes[tag1]
        node2 = self.nodes[tag2]
        return node2.tag in node1.dependencies
         
    def get_successors(self, tag):
        """get the successors (providers) of the node with the given tag."""
        succs = [ioc.stop_node for n in self.nodes.values() 
                for ioc in n.io_connections if ioc.start_node == tag]
        ## also check the ones that are forced dependent on the tag
        succs.extend([n.tag for n in self.nodes.values() 
                      if tag in n.forced_dependencies])
        return list(set(succs))

    def bfs(self, nodes=None, visited=None):
        """breadth first search."""
        ##TODO: check for loops by checking if there is any roots and leafs
        new_nodes = []
        if nodes is None:
            nodes = self.nodes.values()
        if visited is None:
            visited = set()
        for n in nodes:
            if not (n.tag in visited) and set(n.dependencies).issubset(visited):
                visited.add(n.tag)
                yield n.tag
            else:
                new_nodes.append(n)
        if len(new_nodes):
            for tag in self.bfs(new_nodes, visited):
                yield tag 
    
    def deflate(self):
        """cancel parallelization."""
        self._nodes = self._folded_nodes.copy()
        for node in self.nodes.values():
            node.children = []
            node.parallelized = False
        self.inflated = False
 
    def inflate(self):
        """apply expansion, i.e. parallelization or synchronization, to nodes."""
        if self.inflated:
            return
        for tag in self.bfs():
            node = self.nodes[tag]
            if not node.parallelized:
                n = self._unfold(node)
                for child in n.children:
                    self.add_node(child.tag, child)
            
        self._update_forced_dependencies()          
        self._trim()
        self.inflated = True  
     
    def _unfold(self, node):
        ## synchronization happens only for dependencies via io_connections.
        paralleler = Paralleler()
        for ioc in node.io_connections:
            p = self.nodes[ioc.start_node]
            param = ioc.stop_param
            if not p.parallelized:
                continue
            if node.issyncable(p, param):
                n = paralleler.synchronize(node, p, param)
            else:
                m = Merger(node, p, ioc)
                n = m.update_io_connections()
                self.add_node(m.tag, m)
        
        ## expand the node if it is not syncable with any of its predecessors.
        if node.parallel_run and not node.parallelized:
            n = paralleler.expand(node, len(node.chunks))
        else:
            n = node
        return n

    def _update_forced_dependencies(self):
        """update forced_dependencies."""
        def get_new_fd(fd):
            new_fd = []
            for i in range(0, len(fd)):
                p = self.nodes[fd[i]]
                if p.parallelized:
                    new_fd.extend([ch.tag for ch in p.children])
                else:
                    new_fd.append(p.tag)
            return new_fd
        
        for node in self.nodes.values():
            fd = node.forced_dependencies[:]
            if not fd:
                continue
            if node.parallelized:
                for child in node.children:
                    new_fd = get_new_fd(fd)
                    child.forced_dependencies = new_fd
            else:
                new_fd = get_new_fd(fd)
                node.forced_dependencies = new_fd

    def _trim(self):
        """remove parallelized nodes, i.e. parent nodes."""
        for node in self.nodes.values():
            if node.parallelized:
                del self._nodes[node.tag]
        
    def _populate(self):
        """populate attributes by config dictionary."""
        for k, v in self.config_dict.iteritems():
            if not k in ('__PIPELINE_INFO__', '__GENERAL__', '__SHARED__', '__SAMPLES__'):
                node = WorkFlowNode(v)
                node.tag = k
                self._nodes[k] = node
                self._folded_nodes[k] = node
            elif k == '__GENERAL__':
                self._general_section = v
            elif k == '__SHARED__':
                self._shared_section = v
            elif k == '__SAMPLES__':
                self._samples_section = v
            else:
                self._info_section = v
    
    def _add_breakpoint(self, breakpoint_tag, predecessor_tag):
        """create a breakpoint node and force successors of the predecessor node to wait for it."""
        bpnode = BreakPoint(breakpoint_tag, predecessor_tag) 
        ## successor tasks should wait for the break point to run
        successor_tags = self.get_successors(predecessor_tag)
        for t in successor_tags:
            n = self.nodes[t]
            fd = n.forced_dependencies[:]
            fd.extend([bpnode.tag])
            n.forced_dependencies = fd
        self._breakpoints[bpnode.tag] = bpnode
        self.add_node(bpnode.tag, bpnode)

    def _apply_breakpoints(self):
        """add breakpoint nodes to workflow."""
        for node in self.nodes.values():
            if node.breakpoint:
                bptag = "__BREAK_POINT_%s__" % (node.tag)
                self._add_breakpoint(bptag, node.tag)

    def get_samples(self):
        """get samples for factory."""
        if self.samples_section:
            return {sample_id:self._replace_connections(sample_id)
                    for sample_id in self.samples_section.keys()}
        elif self.shared_section:
            return {'__shared__only__':self._replace_connections()}
    
    def _replace_connections(self, sample_id=None):
        """replace connections in each node with its actual value."""
        t = Tree() 
        for tag, node in self.nodes.iteritems():
            for c in node.connections:
                if c.start_node not in ('__SAMPLES__', '__SHARED__'):
                    continue
                
                if c.start_node == '__SAMPLES__':
                    if not self.samples_section:
                        msg = "sample connection used while empty SAMPLES section, %s" % (c) 
                        raise Exception(msg)
                    value = self._get_value_from_section_samples(sample_id, c.start_param)
                elif c.start_node == '__SHARED__':
                    if not self.shared_section:
                        msg = "shared connection used while empty SHARED section, %s" % (c)
                        raise Exception(msg)
                    value = self._get_value_from_section_shared(c.start_param)

                path = c.path                    
                p = path.update_leaf(path, value)
                t[tag].add_path(p)
        
        return t.todict()
         
    def _get_value_from_section_samples(self, sample_id, param):
        value = None
        try:
            value = self.samples_section[sample_id][param]
        except KeyError:
            msg = "bad connection: %s is not a key in SAMPLES section." % (param) 
            print >> sys.stderr, msg
            
        return value

    def _get_value_from_section_shared(self, param):
        value = None
        try:
            value = self.shared_section[param]
        except KeyError:
            msg = "bad connection: %s is not a key in SHARED section." % (param) 
            print >> sys.stderr, msg
            
        return value    

class WorkFlowManager(object):
    
    """
    manage workflows from different config files.
    """

    def __init__(self, config_file=None):
#         self.wf = WorkFlow(config_file)
        self.configurer = Configurer(config_file)
    
    def make_config(self, component_names, config_file_name='pipeline.yaml'):
        """create a yaml config file."""
        config_dict = self.configurer.make_config_dict(component_names)
        self.configurer.print2yaml(config_dict, config_file_name)
        
    def add_workflow(self, config_file):
        """add a new workflow for the given config file."""
        wf = WorkFlow(config_file)
