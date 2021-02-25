# CFSI - Cloud-Free Satellite Imagery

CFSI is a set of Python scripts and tools designed to automatically generate cloudless mosaic images from Sentinel 2 data. It relies heavily on [OpenDataCube](https://www.opendatacube.org/) (ODC) to index Sentinel 2 imagery from [Amazon S3](https://registry.opendata.aws/sentinel-2/) and generate cloudless mosaic images using cloud masks. Cloud detection and masking is done using [S2Cloudless](https://github.com/sentinel-hub/sentinel2-cloud-detector). The mosaics are indexed into ODC and can be served over WMS/WCS using [datacube-ows](https://github.com/GispoCoding/datacube-ows).

## Configuration

CFSI can be configured using a `config.yaml` file. A sample configuration is provided in the repository with comments explaining most options. The configuration file syntax is subject to change without notice until a possible stable release in the future.

## Deployment

### Docker

The easiest way to setup CFSI locally is to use Docker and Docker-Compose. The compose file includes both a backend PostGIS database service and a ODC Python environment. 

First, edit the variables in the .env file to match your system, this applies mainly to the variables ending with `_HOST`.

Then, start CFSI with:
```shell
docker-compose up -d
```

### AWS

A Terraform configuration is also provided to automatically launch and manage CFSI on AWS or other cloud platforms supported by TF.

Set local environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in your profile. To deploy CFSI on AWS, run:
```shell
terraform init
terraform apply
```

### Conda

Another way to set up CFSI locally is to use Conda. Create a new Conda environment using the provided environment.yml file.

## To-Do

The project is still under active development and things may and will change without notice. The to-do list before a possible future stable release includes:

- [ ] CLI for common operations such as generating mosaics and indexing new S2 imagery to ODC
- [ ] Support for other cloud masking methods besides s2cloudless
- [ ] User testing with different areas and configurations
- [ ] Automated test framework and unit tests
