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

variable "test_vpc" {
    default = "vpc-fca71899"
}

variable "test_security_group" {
    default = "sg-13b3a46e"
}

variable "test_subnet" {
    default = "subnet-12afb03a" 
}

resource "aws_instance" "sbt_test" {
    ami =   "${var.kafka-ami}"
    instance_type = "${var.instance_type}"
    associate_public_ip_address = true
    subnet_id = "${var.test_subnet}"
    vpc_security_group_ids = ["${var.test_security_group}"]
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
            "sudo add-apt-repository \"deb https://apt.dockerproject.org/repo/ ubuntu-xenial main\"",
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
}
