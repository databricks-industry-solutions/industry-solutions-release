from databricks_api import DatabricksAPI
import boto3
import logging
import base64
from urllib.parse import quote, unquote
import json
import re
import os

head_section = re.compile("%md[\n\\s]*# (.*)\n?")
part_section = re.compile("%md[\n\\s]*## (.*)\n?")
notebook_regex = re.compile(
    "DATABRICKS_NOTEBOOK_MODEL = '((?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)'")


def create_readme_page(solution_name, local_dir, content):
    content_markdown = base64.b64decode(content['content']).decode('UTF-8')
    content_markdown = f'%md {content_markdown}'
    with open("content/template_readme.json", 'r') as f_in:
        json_object = json.loads(f_in.read())
        json_object['name'] = f'{solution_name} / README'
        json_object['commands'][0]['command'] = content_markdown
        json_b64 = encode_notebook(json_object)
        with open('content/template_readme.html', 'r') as file_in:
            html_text = str(file_in.read())
            notebook_html = html_text.replace('[README_CONTENT]', json_b64)
            with open(f'{local_dir}/README.html', "w") as file_out:
                file_out.write(notebook_html)


def create_index_page(solution_name, local_dir, index_href):
    content_notebook = '%md '
    for notebook_href in index_href:
        content_notebook = '{}\n\n{}'.format(content_notebook, notebook_href)

    with open("content/template_index.json", 'r') as f_in:
        json_object = json.loads(f_in.read())
        json_object['name'] = solution_name

        cmd = '%md '
        for notebook_href in index_href:
            cmd = '{}\n\n{}'.format(cmd, notebook_href)

        json_object['commands'][0]['command'] = cmd
        json_str = json.dumps(json_object)
        json_str = quote(json_str).encode('utf-8')
        json_b64 = base64.b64encode(json_str).decode('UTF-8')
        with open('content/template_index.html', 'r') as file_in:
            output_file = '{}/index.html'.format(local_dir)
            with open(output_file, "w") as file_out:
                html_text = str(file_in.read())
                html_text = enrich_html(html_text)
                html_text = html_text.replace('[NOTEBOOK_HTML_LINK]', 'readme.html')
                html_text = html_text.replace('[NOTEBOOK_CONTENT]', json_b64)
                html_text = html_text.replace('[SOLUTION_NAME]', solution_name)
                file_out.write(html_text)


def encode_notebook(notebook_json):
    notebook_encoded = quote(json.dumps(notebook_json))
    return base64.b64encode(notebook_encoded.encode('utf-8')).decode('utf-8')


def get_section_name(section):
    cmd = section['command']
    if head_section.match(cmd):
        return head_section.match(cmd).group(1)
    elif part_section.match(cmd):
        return part_section.match(cmd).group(1)
    else:
        return 'Context'


def process_notebook_section(org_notebook, section_id, subsection_id, commands):
    new_notebook = org_notebook
    new_notebook['commands'] = commands
    notebook_name = get_section_name(commands[0])
    notebook_encoded = encode_notebook(new_notebook)
    return Section(section_id, subsection_id, notebook_name, notebook_encoded)


def process_notebook_content(notebook_id, notebook_content, notebook_name):
    org_notebook = base64.b64decode(notebook_content).decode('utf-8')
    org_notebook = json.loads(unquote(org_notebook))
    org_notebook['name'] = notebook_name

    commands = []
    notebooks = []
    section_id = notebook_id + 1
    subsection_id = 0

    for command in org_notebook['commands']:

        if not commands:
            commands = [command]
        else:
            command_value = command['command']
            # Start of a new top level section
            if head_section.match(command_value):
                new_notebook = process_notebook_section(org_notebook, section_id, subsection_id, commands)
                notebooks.append(new_notebook)
                commands = [command]
                subsection_id = 0
                section_id += 1
            # Start of a second level section
            elif part_section.match(command_value):
                new_notebook = process_notebook_section(org_notebook, section_id, subsection_id, commands)
                notebooks.append(new_notebook)
                commands = [command]
                subsection_id += 1
            else:
                # following of previous sections
                commands.append(command)

    new_notebook = process_notebook_section(org_notebook, section_id, subsection_id, commands)
    notebooks.append(new_notebook)
    return notebooks


def extract_content(html_content):
    matches = notebook_regex.findall(html_content)
    if matches:
        return matches[0]
    else:
        raise Exception("Could not extract notebook content from HTML")


def transform_html(org_html, notebook_encoded):
    return re.sub(
        notebook_regex,
        "DATABRICKS_NOTEBOOK_MODEL = '{}'".format(notebook_encoded),
        org_html
    )


def valid_file(o):
    if o['object_type'] == "NOTEBOOK":
        if re.compile("^\\d+").match(os.path.basename(o['path'])):
            if not os.path.basename(o['path']).startswith('00'):
                return True
    return False


def enrich_html(html_content):
    html_tag_header = open("content/tag_header.html", "r").read()
    html_tag_body = open("content/tag_body.html", "r").read()
    html_tag_body_end = open("content/tag_body_end.html", "r").read()
    html_content = html_content.replace('<head>', '<head>\n{}'.format(html_tag_header))
    html_content = html_content.replace('<body>', '<body>\n{}'.format(html_tag_body))
    html_content = html_content.replace('</body>', '{}\n</body>'.format(html_tag_body_end))
    return html_content


def create_index(notebook_name, notebook_title):
    return '<a class=index href="{}">{}</a>'.format(notebook_name, notebook_title)


class Section:
    def __init__(self, section_id, subsection_id, notebook_name, notebook_encoded):
        self.section_id = section_id
        self.subsection_id = subsection_id
        self.notebook_name = notebook_name
        self.notebook_encoded = notebook_encoded
        self.logger = logging.getLogger('databricks')
        self.logger.debug("{}/{} - {}".format(section_id, subsection_id, notebook_name))

    def html_name(self, solution_name):
        return "{}_{}-{}.html".format(
            solution_name,
            self.section_id,
            self.subsection_id
        )

    def get_number(self):
        if self.subsection_id == 0:
            return "{}".format(self.section_id)
        else:
            return "{}.{}".format(self.section_id, self.subsection_id)


class Accelerator:

    def __init__(
            self,
            db_host,
            db_token,
            deploy=False,
            aws_s3_bucket=None,
            aws_s3_path=None,
            aws_s3_link=None,
            aws_access_key_id=None,
            aws_secret_access_key=None
    ):

        self.logger = logging.getLogger('databricks')
        self.deploy = deploy

        # read databricks config
        self.db = DatabricksAPI(host=db_host, token=db_token)

        # optionally - read S3 config
        if deploy:

            s3_conf = [x for x in [
                aws_s3_bucket,
                aws_s3_path,
                aws_access_key_id,
                aws_s3_link,
                aws_secret_access_key
            ] if not x]

            if len(s3_conf) > 0:
                raise Exception('Unable to deploy solution to S3, AWS configuration items are invalid')

            aws = boto3.resource('s3',
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key
                                 )
            self.s3_path = aws_s3_path
            self.s3_link = aws_s3_link
            self.s3 = aws.Bucket(aws_s3_bucket)

    def export_to_html(self, remote_path, local_dir, solution_name):
        self.logger.info("Exporting solution accelerator to HTML file(s)")

        objects = self.db.workspace.list(remote_path)['objects']
        notebooks = [o['path'] for o in objects if valid_file(o)]
        index = []
        section_id = 0
        total_notebooks = len(notebooks)

        if total_notebooks == 0:
            raise Exception('Could not find any valid notebook in solution accelerator. Check naming convention')

        self.logger.info("Importing solution [README.md] file")
        readme_file = False
        for o in objects:
            if o['object_type'] == 'FILE' and o['path'].split('/')[-1] == 'README.md':
                readme_content = self.db.workspace.export_workspace(o['path'])
                create_readme_page(solution_name, local_dir, readme_content)
                readme_file = True
                index.append(create_index('readme.html', 'README'))
                break
        if not readme_file:
            raise Exception('Could not find [README.md] file')

        for i, file in enumerate(sorted(notebooks)):

            html_content = self.db.workspace.export_workspace(file, format='HTML')
            html_text = str(base64.b64decode(html_content['content']), 'utf-8')
            notebook_name = '{} / {}'.format(
                solution_name,
                file.split('/')[-1]
            )
            self.logger.info("Processing notebook {}/{} [{}]".format(i + 1, total_notebooks, file.split('/')[-1]))
            notebook = extract_content(html_text)
            children = process_notebook_content(section_id, notebook, notebook_name)
            for child in children:
                if child.section_id > section_id:
                    section_id = child.section_id
                child_name = child.html_name(solution_name)
                with open("{}/{}".format(local_dir, child_name), 'w') as f_out:
                    child_html = transform_html(html_text, child.notebook_encoded)
                    f_out.write(child_html)
                    child_title = "{} {}".format(child.get_number(), child.notebook_name)
                    index.append(create_index(child_name, child_title))

        self.logger.info("Create Index page")
        create_index_page(solution_name, local_dir, index)

    def deploy_s3(self, files, solution_codename):
        for file in files:
            file_name = os.path.basename(file)
            remote_file = self.s3_path.format(
                solution_codename=solution_codename,
                file_name=file_name
            )
            self.logger.info(f"Publishing [{file_name}] to s3")
            with open(file, 'r') as f:
                data = f.read()
                self.s3.put_object(
                    Key=remote_file,
                    Body=data,
                    ACL='public-read',
                    ContentType='text/html'
                )

    def release(self, db_path, db_name, output_dir):
        self.logger.info(f"Releasing solution [{db_name}]")
        self.export_to_html(db_path, output_dir, db_name)
        if self.deploy:
            files = os.listdir(output_dir)
            self.deploy_s3([f"{db_path}/{file}" for file in files], db_name)
            self.logger.info("Solution deployed to [{link}/{solution_codename}/index.html]".format(
                link=self.s3_link,
                solution_codename=db_name)
            )
