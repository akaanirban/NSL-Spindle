variable "ssh_path" {
    default = "~/.ssh/id_rsa"
}

variable "ssh_key_name" {
    default = "RoryMacbookPro"
}

variable "instance_type" {
    #default = "r3.8xlarge"
    default = "t2.small"
}

resource "aws_vpc" "nsl_test_vpc" {
    cidr_block = "192.168.0.0/24"
    enable_dns_support = true
    enable_dns_hostnames = true
}

resource "aws_subnet" "nsl_test_subnet" {
    vpc_id = "${aws_vpc.nsl_test_vpc.id}"
    cidr_block = "192.168.0.0/24"
    map_public_ip_on_launch = true
}

resource "aws_security_group" "nsl_test_allow_ssh" {
    name = "nsl_test_allow_ssh"
    vpc_id = "${aws_vpc.nsl_test_vpc.id}"
    ingress {
        from_port = 22
        to_port = 22
        protocol = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }
    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

resource "aws_internet_gateway" "gw" {
    vpc_id = "${aws_vpc.nsl_test_vpc.id}"
}

resource "aws_route_table" "rt" {
    vpc_id = "${aws_vpc.nsl_test_vpc.id}"
    route {
        cidr_block = "0.0.0.0/0"
        gateway_id = "${aws_internet_gateway.gw.id}"
    }
}

resource "aws_route" "internet_route" {
    route_table_id = "${aws_route_table.rt.id}"
    destination_cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.gw.id}"
}

resource "aws_eip" "nat_ip" {
    vpc = true
    depends_on = ["aws_internet_gateway.gw"]
}

resource "aws_nat_gateway" "nat_gw" {
    allocation_id = "${aws_eip.nat_ip.id}"
    subnet_id = "${aws_subnet.nsl_test_subnet.id}"
    depends_on = ["aws_internet_gateway.gw"]
}

resource "aws_route_table_association" "internet" {
    subnet_id = "${aws_subnet.nsl_test_subnet.id}"
    route_table_id = "${aws_route_table.rt.id}"
}

/*resource "aws_instance" "sbt_test" {
    ami =   "${var.kafka-ami}"
    depends_on = ["aws_internet_gateway.gw"]
    instance_type = "${var.instance_type}"
    associate_public_ip_address = true
    subnet_id = "${aws_subnet.nsl_test_subnet.id}"
    vpc_security_group_ids = ["${aws_security_group.nsl_test_allow_ssh.id}"]
    key_name = "${var.ssh_key_name}"
    root_block_device {
        volume_size =   120
    }
    provisioner "remote-exec" {
        inline = [
            "echo \"deb https://dl.bintray.com/sbt/debian /\" | sudo tee -a /etc/apt/sources.list.d/sbt.list",
            "sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823",
            "sudo apt-get update",
            "sudo apt-get -y upgrade",
            "sudo apt-get -y install sbt curl linux-image-extra-$(uname -r) linux-image-extra-virtual apt-transport-https ca-certificates",
            "curl -s http://yum.dockerproject.org/gpg | sudo apt-key add",
            "sudo add-apt-repository \"deb https://apt.dockerproject.org/repo/pool/ $(lsb_release -cs) main",
            "sudo apt-get update",
            "sudo apt-get -y install docker-engine"
        ]
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
}


output "connect_ip" {
    value = "${aws_instance.sbt_test.public_ip}"
}*/
