## Purpose of the library
This library was created in order to create a common way to execute sql commands on the NREMC SQL Server from local Python scripts.

## Warning
The SQL Server connection is not encrypted so I do not recommend using this library to connect to a server that is not locally hosted.

## Why this library was written
While writing code for NREMC I constantly had to connect to the NREMC SQL Server but every time I did it I always did it in a different way. I created library in order to create a unified way to connect to a SQL Server specifically the NREMC server.