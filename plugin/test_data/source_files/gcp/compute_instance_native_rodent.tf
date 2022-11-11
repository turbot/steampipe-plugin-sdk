resource "google_compute_network" "compute_network_native_rodent" {
  name = "native-rodent"
  auto_create_subnetworks = true
}

resource "google_compute_instance" "compute_instance_native_rodent" {
  name         = "native-rodent"
  machine_type = "f1-micro"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = google_compute_network.compute_instance_native_rodent.name
  }

  metadata = {
    block-project-ssh-keys = true
  }

  labels = {
    name = "native-rodent"
  }
}

output "compute_instance_native_rodent" {
  value = google_compute_instance.compute_instance_native_rodent.id
}
