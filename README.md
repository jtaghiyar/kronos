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

###2.3.0
* added a mechanism to update the requirements of the implicit merge via GENERAL section. **Note:** you can switch off the implicit merge and use an explicit merge if you want to specify a particular requirement for only that merge task.
* previously, multiple identical implicit merge tasks could exist in a workflow. In the new version, they are combined into a single merge task that happens only once. 
* added a switch called ```merge``` that defaults to ```True``` in the _run_ subsection of each task. If it is set to ```False```, the implicit merging mechanism is switched off for that task and the following warning message is shown when initializing the workflow:

```
UserWarning: Implicit merge is off for task <the_task_name>. You may have to use an explicit merge task.
```

* added support for tags in the interval file, _i.e._ an optional tag can be added for each chunk in each line of the interval file that will be used as the suffix for the name of the task corresponding to that chunk. The tags should be added to each line using tab as the separation character, e.g.

```
chunk1	tag1
chunk2	tag2
chunk3
chunk4 tag4
```

* bug fixes.

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
