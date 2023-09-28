"""
* upload part should be externally create / read only
* upload / file handle should derive the content_type from the file name
* upload should have a post processing trigger that deletes parts if there is no associated file handle
* file handle should have a post processing trigger that deletes parts
* we need a periodic task to clear expired uploads and files
* file handle should be externally read only
"""