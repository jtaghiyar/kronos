'''
Created on March 10, 2014

@author: jtaghiyar
'''

import argparse
import os
from .kronos_version import kronos_version
 
parser = argparse.ArgumentParser(description="""Kronos: a workflow assembler
                                             for genome analytics and
                                             informatics""")

subparsers = parser.add_subparsers(dest='subparser_name')

parser.add_argument('-w', '--working_dir',
                    default=os.path.join(os.getcwd()),
                    help="path to the working dir")

parser.add_argument('-v', '--version', 
                    action='version',
                    version=kronos_version)

## make_component subparser
parser_make_component = subparsers.add_parser('make_component',
                                              description="make a template component",
                                              help="make a template component")

parser_make_component.add_argument('component_name',
                                   help="a name for the component to be generated")

## make_config subparser
parser_make_config = subparsers.add_parser('make_config', 
                                           description="make a template config file",
                                           help="make a template config file")

parser_make_config.add_argument('components',
                                nargs='+',
                                help="list of component names")

parser_make_config.add_argument('-o', '--output_filename',
                                required=True,
                                help="a name for the resultant config file")

## update_config subparser
parser_update_config = subparsers.add_parser('update_config',
                                             description="""update the fields of a config
                                             file based on the ones from another one""",
                                              help="""update the fields of a config
                                             file based on the ones from another one""")

parser_update_config.add_argument('config_files',
                                   nargs='+',
                                   metavar='FILE',
                                   help="""paths to the config files,
                                   e.g. update_config <old_config.yaml> <new_config.yaml>""")

parser_update_config.add_argument('-o', '--output_filename',
                                   required=True,
                                   help="a name for the output file")

## init_pipeline subparser 
parser_init_pipeline = subparsers.add_parser('init',
                                             description="initialize a pipeline from a given config file",
                                             help="initialize a pipeline from a given config file")

parser_init_pipeline.add_argument('-e', '--pipeline_name',
                                  required=True,
                                  help="a name for the resultant pipeline")

parser_init_pipeline.add_argument('-i', '--input_samples',
                                  default=None,
                                  metavar='FILE',
                                  help="path to the samples file")

parser_init_pipeline.add_argument('-s', '--setup_file',
                                  default=None,
                                  metavar='FILE',
                                  help="path to the setup file")

parser_init_pipeline.add_argument('-y', '--config_file',
                                  required=True,
                                  metavar='FILE',
                                  help="path to the config_file.yaml")

## run_pipeline subparser
parser_run_pipeline = subparsers.add_parser('run',
                                            description="""run kronos-made pipelines 
                                            with optional initialization""",
                                            help="""run kronos-made pipelines 
                                            with optional initialization""")

parser_run_pipeline.add_argument('-b', '--job_scheduler',
                                 default='drmaa',
                                 choices=['sge','drmaa'],
                                 help="job scheduler used to manage jobs on the cluster")

parser_run_pipeline.add_argument('-c', '--components_dir',
                                 default=os.getcwd(), 
                                 required=True, 
                                 help="path to components_dir")

parser_run_pipeline.add_argument('-d', '--drmaa_library_path',
                                 default='lib/lx24-amd64/libdrmaa.so',
                                 type=str,
                                 help="path of drmaa library")

parser_run_pipeline.add_argument('-e', '--pipeline_name',
                                 default=None,
                                 type=str, 
                                 help="pipeline name")

parser_run_pipeline.add_argument('-i', '--input_samples',
                                 default=None,
                                 metavar='FILE', 
                                 help="path to the input samples file")

parser_run_pipeline.add_argument('-j', '--num_jobs',
                                 default=1,
                                 type=int, 
                                 help='maximum number of simultaneous jobs per pipeline')

parser_run_pipeline.add_argument('-k', '--kronos_pipeline',
                                 default=None,
                                 metavar='FILE',
                                 help='path to kronos-made pipeline script.')

parser_run_pipeline.add_argument('-n', '--num_pipelines',
                                 default=1,
                                 type=int,
                                 help='maximum number of simultaneous running pipelines')

parser_run_pipeline.add_argument('-p','--python_installation',
                                 default='python',
                                 type=str,
                                 help="path to python executable")

parser_run_pipeline.add_argument('-q', '--qsub_options',
                                 default=None,
                                 type=str, 
                                 help="""native qsub specifications for the cluster
                                        in a single string""")

parser_run_pipeline.add_argument('-r', '--run_id',
                                 default=None,
                                 type=str, 
                                 help="pipeline run id")

parser_run_pipeline.add_argument('-s', '--setup_file',
                                 default=None,
                                 metavar='FILE',
                                 help="path to the setup file")

parser_run_pipeline.add_argument('-w', '--working_dir',
                                 default=os.getcwd(), 
                                 help="path to the working_dir")

parser_run_pipeline.add_argument('-y', '--config_file',
                                 default=None,
                                 metavar='FILE',
                                 help="path to the config_file.yaml")

parser_run_pipeline.add_argument('--no_prefix',
                                  default=False,
                                  action='store_true',
                                  help="""switch off the prefix that is added to all the
                                          output files.""")
# ## test subparser
# parser_test = subparsers.add_parser('test',
#                                     description="test kronos installation by running tests included in the package",
#                                     help="test kronos installation by running tests included in the package")
# 
# parser_test.add_argument('-i', '--python_installation', type=str,
#                          default='python',
#                          help="python executable")

args, unknown = parser.parse_known_args()
