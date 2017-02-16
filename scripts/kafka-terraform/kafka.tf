variable "kafka_count" {
    default = 0
}

variable "kafka_security_group" {
    default = "sg-13b3a46e"
}

variable "kafka_subnet" {
    default = "subnet-12afb03a" 
}

variable "kafka_vpc" {
    default = "vpc-fca71899"
}

variable "kafka_instance_type" {
    default = "d2.xlarge"
}

variable "ssh_path" {
    default = "~/.ssh/id_rsa"
}

variable "ssh_key_name" {
    default = "RoryMacbookPro"
}

resource "aws_instance" "kafka_compute" {
    ami =   "${var.kafka-ami}"
    instance_type = "${var.kafka_instance_type}"
    count = "${var.kafka_count}"
    associate_public_ip_address = true
    subnet_id = "${var.kafka_subnet}" 
    vpc_security_group_ids = ["${var.kafka_security_group}", "${aws_security_group.allow_all_kafka.id}", "${aws_security_group.allow_all_zookeeper.id}"] // Allow all intra-group traffic and all traffic from my IP
    key_name = "${var.ssh_key_name}"
    root_block_device {
        volume_size =   120
    }
    provisioner "file" {
        source = "scripts"
        destination = "/home/ubuntu"
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
    // Inline broker config
    provisioner "file" {
        content = <<EOF
${file("scripts/config/server.properties")}
advertised.listeners=PLAINTEXT://${self.public_ip}:9092
listeners=PLAINTEXT://${self.private_ip}:9092
broker.id=${count.index}
zookeeper.connect=${aws_instance.zookeeper_compute.public_ip}:2181
EOF
        destination = "/home/ubuntu/scripts/config/server.properties"
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
    provisioner "remote-exec" {
        inline = [
            "chmod +x scripts/*.sh",
            "curl -sL https://deb.nodesource.com/setup_7.x | sudo -E bash -",
            "sudo apt-get install -y nodejs",
            "cd scripts && sudo ./installJava.sh && sudo ./installKafka.sh && sudo node ./fork.js bash ./startKafka.sh"
        ]
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
}

resource "aws_instance" "zookeeper_compute" {
    ami =   "${var.kafka-ami}"
    instance_type = "t2.medium"
    associate_public_ip_address = true
    subnet_id = "${var.kafka_subnet}"
    vpc_security_group_ids = ["${var.kafka_security_group}", "${aws_security_group.allow_all_kafka.id}"] // Allow all intra-group traffic and all traffic from my IP
    key_name = "${var.ssh_key_name}"
    root_block_device {
        volume_size =   120
    }
    provisioner "file" {
        source = "scripts"
        destination = "/home/ubuntu"
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
    provisioner "remote-exec" {
        inline = [
            "chmod +x scripts/*.sh",
            "curl -sL https://deb.nodesource.com/setup_7.x | sudo -E bash -",
            "sudo apt-get install -y nodejs",
            "cd scripts && sudo ./installJava.sh && sudo ./installKafka.sh && sudo node ./fork.js bash ./startZk.sh"
        ]
        connection {
            type = "ssh"
            user = "ubuntu"
            private_key = "${file("${var.ssh_path}")}"
        }
    }
}


resource "aws_security_group" "allow_all_kafka" {
    name = "nsl-allow-kafka-traffic"
    description = "Allow communication over public IPs"
    vpc_id = "${var.kafka_vpc}"
}

resource "aws_security_group_rule" "allow_traffic" {
    type = "ingress"
    from_port = 0
    to_port = 65535 
    protocol = "tcp"
    cidr_blocks = ["${formatlist("%s/32", aws_instance.kafka_compute.*.public_ip)}"]
    security_group_id = "${aws_security_group.allow_all_kafka.id}"
}

resource "aws_security_group" "allow_all_zookeeper" {
    name = "nsl-allow-zookeeper-traffic"
    description = "Allow communication over public IPs"
    vpc_id = "${var.kafka_vpc}"
}

resource "aws_security_group_rule" "allow_zookeeper" {
    type = "ingress"
    from_port = 0
    to_port = 65535 
    protocol = "tcp"
    cidr_blocks = ["${aws_instance.zookeeper_compute.public_ip}/32"]
    security_group_id = "${aws_security_group.allow_all_zookeeper.id}"
}

output "kafka" {
    value = "${join(",", formatlist("%s:9092", aws_instance.kafka_compute.*.public_ip))}"
}
output "zookeeper" {
    value = "${format("%s:2181", aws_instance.zookeeper_compute.public_ip)}"
}
