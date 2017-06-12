Hostname: gpfs1
VS Login: ssh root@50.97.244.187
Password: GJb8kHne

+-----------------------------------+----------------------------------------------+
| FILE                              | DESCRIPTION                                  |
+-----------------------------------+----------------------------------------------+
| /root/hw4setup.sh                 | Uses wget to download zipped two-gram files  |
+-----------------------------------+----------------------------------------------+
| /root/hw4preprocess.sh            | Uses awk to sum two-gram record counts       |
+-----------------------------------+----------------------------------------------+
| /root/mumbler.py                  | Mumbler program that requires 2 inputs       |
+-----------------------------------+----------------------------------------------+
| /gpfs/gpfsfpo/zipped_files/       | File storage location for zipped files       |
+-----------------------------------+----------------------------------------------+
| /gpfs/gpfsfpo/preprocessed_files/ | File storage location for preprocessed files |
+-----------------------------------+----------------------------------------------+

The setup scripts (hw4setup.sh, hw4preprocess.sh) have already been run to download 
the zip files and preprocess them in the cluster. The Mumbler program is already set 
up in root and can be run directly in there.

Mumbler program syntax: python mumbler.py <word> <number>
Example: python mumbler.py water 5
