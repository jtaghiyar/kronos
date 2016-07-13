# Kronos 

## About

A workflow assembler for genome analytics and informatics.

## Documentation

Please refer to kronos documentation here: <http://kronos.readthedocs.org/en/latest/>

## Installation

Using pip:

```
pip install kronos_pipeliner
```

## Dependencies

* [Python == 2.7.5](http://www.python.org)

### Python libraries

* [ruffus == 2.4.1](http://www.ruffus.org.uk/)

* [PyYaml == 3.11 ](http://pyyaml.org/)

### Optional Python Libraries

For running on a cluster using `-b/--job_scheduler drmaa` you will need to install:

* [drmaa-python == 0.7.6](http://drmaa-python.github.io)

## Questions and feedback

Please use our [kronos google group](https://groups.google.com/forum/#!forum/kronos_pipeliner).

## Contact

Jafar Taghiyar <jafar.taghiyar@gmail.com>.

## Change log

###2.2.0
* each task in the configuration file now has its own _requirements_ entry in the _run_ subsection which takes precedence over the requirements listed in the _GENERAL_ section. This enables users to have different versions of the same requirements for different tasks.
* interval file now takes precedence over the synchronization, i.e. if a task has an interval file, then it will not be synchronized with its predecessors. 
* added support for floating point memory requests.
* made all the merged files to store in a directory called _merge_.
* username and version are automatically added to the config files when using _make_config_ command.
* added a check to make sure that the input of the implicit merge node is always a list.
* bug fixes.

###2.1.0
* Kronos now uses multithreading instead of multiprocessing.

### 2.0.4
* removed the limitation on the number of simultaneous jobs/pipelines.
* added ```--no_prefix``` back to the input options of ```run``` command. 
* minor bug fixes.

### 2.0.3
First version released!
