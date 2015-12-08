""" 
component_main.py
This module contains Component class which extends 
the ComponentAbstract class. It is the core of a component.

Note the places you need to change to make it work for you. 
They are marked with keyword 'TODO'.
"""

from kronos.utils import ComponentAbstract
import os


class Component(ComponentAbstract):
    
    """
    TODO: add component doc here. 
    """

    def __init__(self, component_name="$COMPONENT_NAME", 
                 component_parent_dir=None, seed_dir=None):
        
        ## TODO: pass the version of the component here.
        self.version = "v0.99.0"

        ## initialize ComponentAbstract
        super(Component, self).__init__(component_name, 
                                        component_parent_dir, seed_dir)

    ## TODO: write the focus method if the component is parallelizable.
    ## Note that it should return cmd, cmd_args.
    def focus(self, cmd, cmd_args, chunk):
        pass 
#         return cmd, cmd_args

    ## TODO: this method should make the command and command arguments 
    ## used to run the component_seed via the command line. Note that 
    ## it should return cmd, cmd_args. 
    def make_cmd(self, chunk=None):
        ## TODO: replace 'comp_req' with the actual component
        ## requirement, e.g. 'python', 'java', etc.
        cmd = self.requirements['comp_req']
        
        cmd_args = []

        args = vars(self.args)

        ## TODO: fill the following component params to seed params dictionary
        ## if the name of parameters of the seed are different than
        ## component parameter names.
        comp_seed_map = {
                         #e.g. 'component_param1': 'seedParam1',
                         #e.g. 'component_param2': 'seedParam2',
                        }

        for k, v in args.items():
            if v is None or v is False:
                continue

            ## TODO: uncomment the next line if you are using
            ## comp_seed_map dictionary.
            # k = comp_seed_map[k]            
            
            cmd_args.append('--' + k)
            
            if isinstance(v, bool):
                continue
            if isinstance(v, str):
                v = repr(v)
            if isinstance(v, (list, tuple)):
                cmd_args.extend(v)
            else:
                cmd_args.extend([v])
        
        if chunk is not None:
            cmd, cmd_args = self.focus(cmd, cmd_args, chunk)
            
        return cmd, cmd_args

## To run as stand alone
def _main():
    c = Component()
    c.args = component_ui.args
    c.run()

if __name__ == '__main__':
    import component_ui
    _main()

