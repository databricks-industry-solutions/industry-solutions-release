# Solution accelerator deployment

Options are as follows

- `-c` (`--config`) path to a valid YAML configuration file, see below example
- `-s` (`--solution`) code name of the solution, HTTP compatible
- `-p` (`--path`) path of the solution to download from databricks
- `-v` (`--vertical`) code name of the vertical, can be any of [fsi, hls, mae, cpg, mnf]
- `-d` (`--deploy`) flag to deploy to S3 or not
- `-m` (`--markdown`) whether we want to split notebooks based on markdown

Code is executed as follows

```shell
python databricks.py \
  --config config.yaml \
  --solution esg_scoring \
  --vertical fsi \
  --markdown \
  --path /Repos/antoine.amend@databricks.com/gtm-solution-accelerators/fsi/esg_scoring \
  --deploy
```

Here is an example of YAML configuration

```yaml
databricks:
  host: e2-demo-west.cloud.databricks.com
  token: dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX

aws:
  access_key_id: YYYYYYYYYYYYYYYYYYYYYYYY
  secret_access_key: YYYYYYYYYYYYYYYYYYYY
  bucket:
    name: databricks-web-files
    path: notebooks/{vertical}/{solution_codename}/{file_name}
    link: https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks
```

This will result in HTML files + DBC archive + Manifest files deployed on S3 and publicly available. 
In addition to deploying HTML files, we also modify original content with tag management and lead collector JS.

```
https://databricks-web-files.s3.us-east-2.amazonaws.com/notebooks/{vertical}/{solution}/index.html
```

### Authors

<antoine.amend@databricks.com>
