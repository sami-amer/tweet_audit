# Tweet Auditor

Useability roadmap:
[x] add a template .server file
[x] update Rust code to accept env variables for Postgres
- make a python script that reads .server file and makes it into env variables
- make Toolkit and handler available on the command line
[x] make a script that reads from a per-line .txt to update or remove users
- group together postgres functions (i.e. adding/deleting users should update tables automatically)
- make a function that will clean up local id_name_mapping
- make a proper readme :)