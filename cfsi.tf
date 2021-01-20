terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "eu-central-1"
}

variable "AWS_ACCESS_KEY_ID" {
  description = "AWS access key id"
  type = string
  sensitive = true
}
variable "AWS_SECRET_ACCESS_KEY" {
  description = "AWS secret access key"
  type = string
  sensitive = true
}

resource "aws_vpc" "cfsi_vpc" {
  cidr_block = "172.27.0.0/16"
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_vpc"
    Project = "UNCFSI"
  }
}

resource "aws_internet_gateway" "cfsi_server_gateway" {
  vpc_id = aws_vpc.cfsi_vpc.id
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_server_gateway"
    Project = "UNCFSI"
  }
}

resource "aws_subnet" "cfsi_server_subnet" {
  availability_zone = "eu-central-1a"
  cidr_block = "172.27.27.0/24"
  map_public_ip_on_launch = "true"
  vpc_id = aws_vpc.cfsi_vpc.id
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_server_subnet"
    Project = "UNCFSI"
  }
}

resource "aws_route_table" "cfsi_server_route_table" {
  vpc_id = aws_vpc.cfsi_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.cfsi_server_gateway.id
  }
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_server_route_table"
    Project = "UNCFSI"
  }
}

resource "aws_route_table_association" "cfsi_route_table_assoc" {
  route_table_id = aws_route_table.cfsi_server_route_table.id
  subnet_id = aws_subnet.cfsi_server_subnet.id
}

resource "aws_security_group" "cfsi_sec_group" {
  name = "cfsi_sec_group"
  vpc_id = aws_vpc.cfsi_vpc.id
  egress {
    cidr_blocks = ["0.0.0.0/0"]
    from_port = 0
    protocol = "-1"
    to_port = 0
  }
  ingress {
    cidr_blocks = ["0.0.0.0/0"]
    description = "ssh"
    from_port = 22
    protocol = "tcp"
    to_port = 22
  }
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_sec_group"
    Project = "UNCFSI"
  }
}

resource "aws_network_interface" "cfsi_server_nic" {
  private_ips = ["172.27.27.27"]
  security_groups = [aws_security_group.cfsi_sec_group.id]
  subnet_id = aws_subnet.cfsi_server_subnet.id
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi_server_nic"
    Project = "UNCFSI"
  }
}

resource "aws_instance" "cfsi_server" {
  ami = "ami-0502e817a62226e03"
  availability_zone = "eu-central-1a"
  instance_type = "c5a.16xlarge"
  key_name = "cfsi"
  connection {
    host = self.public_ip
    private_key = file("~/.ssh/cfsi.pem")
    type = "ssh"
    user = "ubuntu"
  }
  network_interface {
    device_index = 0
    network_interface_id = aws_network_interface.cfsi_server_nic.id
  }
  tags = {
    Customer = "UNCFSI"
    Name = "cfsi-server-tf"
    Project = "UNCFSI"
  }
  root_block_device { volume_size = 80 }
  user_data = templatefile("${path.module}/setup_cfsi.tpl", {
    AWS_ACCESS_KEY_ID = var.AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY = var.AWS_SECRET_ACCESS_KEY
  })
}

resource "aws_s3_bucket" "cfsi_s3" {
  acl = "private"
  bucket = "cfsi-s3"
  tags = {
    Customer = "UNCFSI"
    Name = "CFSI S3"
    Project = "UNCFSI"
  }
}

output "cfsi_server_ip_addr" {
  value = aws_instance.cfsi_server.public_ip
}
