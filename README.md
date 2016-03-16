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

###2.1.0
* Kronos now uses multithreading instead of multiprocessing.

### 2.0.4
* removed the limitation on the number of simultaneous jobs/pipelines.
* added ```--no_prefix``` back to the input options of ```run``` command. 
* minor bug fixes.

### 2.0.3
First version released!
