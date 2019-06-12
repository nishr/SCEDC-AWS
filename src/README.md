# Scripts for Seismic Computing SCEDC Data on AWS cloud

## Steps to get up and running with Python and/or Julia

1. Log into AWS using 

2. Launch a fresh EC2 instance
- Step 1 Choose AMI: Select Ubuntu Server 18.04 LTS
- Step 2 Choose an Instance Type: r5.large ~ m5a.large are good for build & testing. 
- Step 3 Configure Instance Details: Turn on IAM role to access S3 to avoid using AWS credentials on EC2. **Do not leave AWS credentials on public AMI**
- Step 4 Add Storage: Increase EBS volume size to 100 GB to host sample mseed data.

3. Pull data from S3. 

4. Install Python and Julia using:
- Anaconda python environment. See [./build_environment/python](./build_environment/python)
- Julia installer. See[./build_environemt/julia](./build_environment/julia)

5. Run sample Python/Julia code on mseed data.