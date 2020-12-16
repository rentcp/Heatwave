# Heatwave

A Python Analysis CLI Tool designed for Aqua AIRS `.hdf` files

Commissioned by Chris Rentsch 



**Getting Started (Windows)**

1. Install Anaconda
2. Start an Anaconda Prompt
3. Install dependencies:


```
conda install py-hdf -c conda-forge
conda install requests
conda install pytest
pip install asciimatics
pip install pytest-randomly
conda install matplotlib
conda install pandas
```

**Getting Started (Linux)**

Most dependencies can be installed with pip, but the HDF4 development libraries must be installed on your system first.

In Ubuntu / Debian-based systems, the required library can be found in the libhdf4-dev package, e.g.:
```
sudo apt install libhdf4-dev
```

In Fedora / CentOS / RHEL, it can be found in the 'hdf-devel' package, e.g.:
```
sudo dnf install hdf-devel
```

Otherwise, the library must be compiled from [source](https://support.hdfgroup.org/products/hdf4/).


Then, install pyhdf within an Anaconda virtual environment:
```
conda install -c conda-forge pyhdf
```


Finally, the rest of the requirements can be installed with:

```
pip install -r requirements/common.txt
```


**Example execution**
```
python cli.py
```

The program will display the list of inputs required, their ranges, and their "<" versus "<=" distinctions.
Use a text editor to set preferred parameters in example.json (you can name this anything.json)

```
python cli.py example.json
```

Enter your Earthdata login and password
Program will run according to the settings in example.json file.

### What is the output?

There are two files:
1. `radiance_wavelength_month_2019-09-02_21-48-53_concatenated.csv`
2. `radiance_wavelength_month_2019-09-02_21-48-53_stats.csv`

Each file has the same dimensions.
- File 1 contains the average radiance for each latitude bin for each AIRS channel for each month processed.
- File 2 contains the count of radiance measurements that contributed to the average in file 1.

### Running tests
To run these tests, this is the command that works on my system (requires pytest to be installed):
```
python -m pytest tests/
```
By default, this will capture and hide any information that is normally printed to the console. If a test fails, check the verbose output by adding the -s flag:
```
python -m pytest tests/ -s
```
Adding new tests
To add a new test, add a pair of files 'your_test.json' and same-named 'your_test.csv' with the input JSON data and expected granule selection info, respectively. Check the included example under 'tests/granule_selection_test_data/example_test.json'.

As soon as new test JSON and CSV data is added to this directory, a test is automatically added and will be run by pytest.

### Versioning
Version 17 was released onto GitHub for public consumption.
For the latest versions available, see the tags on this repository.

### Authors
- Leon De Almeida - Initial work, 2 months - Freelancer.com
- Robert Amours - Subsequent work, 14+ months - Freelancer.com

### License
This project is licensed under the MIT License - see the LICENSE.md file for details
