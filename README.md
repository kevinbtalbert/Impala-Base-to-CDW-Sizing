# impala_workload_sizing

The purpose of this tool is to help size impala workloads for CDW in the private cloud data services.  <br/>
The tool consists of a configuration file which contains the following:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;password file location (base 64 encoded)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;cluster url<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;time from<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;time to<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;username<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;output file locations <br/>
<br/><br/>
The impala sizing script contains the t-shirt size dictionaries.  It makes API calls to CM and pulls a series of queries over the given time frame and outputs a csv which can be consumed to match the query to the proper t-shrt size as well as constraints on the workload.
