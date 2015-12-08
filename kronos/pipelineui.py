'''
Created on May 9, 2014

@author: jtaghiyar
'''

import argparse
import os

parser = argparse.ArgumentParser(description='Pipeline user interface')

parser.add_argument('-b', '--job_scheduler',
                     default='drmaa',
                     choices=['sge','drmaa'],
                     help="job scheduler used to manage jobs on the cluster")

parser.add_argument('-c', '--components_dir',
                     default=os.getcwd(), 
                     required=True, 
                     help="path to components_dir")

parser.add_argument('-d', '--drmaa_library_path',
                     default='lib/lx24-amd64/libdrmaa.so',
                     type=str,
                     help="path of drmaa library")

parser.add_argument('-e', '--pipeline_name',
                     default=None,
                     type=str, 
                     help="pipeline name")

parser.add_argument('-j', '--num_jobs',
                     default=1,
                     type=int, 
                     help='maximum number of simultaneous jobs per pipeline')

parser.add_argument('-l', '--log_file',
                    default=None,
                    type=str,
                    help="name of the log file")

parser.add_argument('-n', '--num_pipelines',
                     default=1,
                     type=int,
                     help='maximum number of simultaneous running pipelines')

parser.add_argument('--no_prefix',
                    default=False,
                    action='store_true',
                    help="""Switch off the prefix that is added to all the
                            output files.""")

parser.add_argument('-p','--python_installation',
                     default='python',
                     type=str,
                     help="python executable")

parser.add_argument('-q', '--qsub_options',
                     default=None,
                     type=str, 
                     help="""native qsub specifications for the cluster
                             in a single string""")

parser.add_argument('-r', '--run_id',
                     default=None,
                     type=str, 
                     help="pipeline run id used for re-running")

parser.add_argument('-w', '--working_dir',
                     default=os.getcwd(), 
                     help="path to the working_dir")

## should be moved to a subcommand print
parser.add_argument('--draw_vertically',
                    default=False,
                    action='store_true',
                    help="specify whether to draw the plot vertically")

parser.add_argument('--extension',
                    default="png",
                    type=str,
                    help="specify the desired extension of the resultant file")

parser.add_argument('--no_key_legend',
                    default=False,
                    action='store_true',
                    help="if True, hide the legend.")

parser.add_argument('--print_only',
                    default=False,
                    action='store_true', 
                    help="""if True, print the workflow graph only without
                    running the pipeline.""")

args, unknown= parser.parse_known_args()
