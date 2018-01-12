# BGP-age-false-positive-study
This repository is designed to compute the false positives of various age-based BGP monitoring systems for use by certificate authorities.

The script in this repository contains a more advanced version of the algorithm outlined in: https://www.princeton.edu/~pmittal/publications/bgp-tls-hotpets17.

This algorithm must be run with the BGPStream python library properly installed (https://bgpstream.caida.org/) and the MySQLdb library installed (http://mysql-python.sourceforge.net/MySQLdb.html).

The script must also be run with a properly formatted SQL database. Database backups and schema structures for the database can be found in the (pending refernce) Git repo.

The main module for this script is test_certs.py. All other .py files are not meant to be called directly. 
