# Solution accelerator deployment

Options are as follows

- `-c` (`--config`) path to a valid YAML configuration file, see below example
- `-n` (`--name`) code name of the solution, HTTP compatible
- `-p` (`--path`) path of the solution to download from databricks
- `-v` (`--vertical`) code name of the vertical, can be any of [fsi, hls, cme, cpg, mnf]
- `-d` (`--deploy`) flag to deploy to S3 or not

Code is executed as follows

```shell
python databricks.py \
  --solution esg_scoring \
  --vertical fsi \
  --markdown \
  --path /Repos/antoine.amend@databricks.com/gtm-solution-accelerators/fsi/esg_scoring \
  --deploy
```

This will result in HTML files deployed on S3 and publicly available. 
In addition to deploying HTML files, we also modify original content with tag management and lead collector JS.

```
https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks/{vertical}/{solution}/index.html
```

### Authors

<antoine.amend@databricks.com>
