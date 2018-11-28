A system contains multiple data sources, every data source would generate data endless. data source would send two messages to itself streaming in order for every data

prepare message(type == prepare), contains prepare token
commit message(type == message), contains prepare andcommit token
requirements

sort data by commit token ascending, and output them in the fastest way you can implement(this system has a real time requirement)
add flow control for your code (data are endless, I think we need to control memory and cpu usage)
I have complete code relate data source, you just need to complete sort and output code in collect(), and you can add some auxiliary structures, variables and functions, but don't modify any definitions.
