resource "google_compute_network" "compute_network_actual_earwig" {
  name = "actual-earwig"
  auto_create_subnetworks = true
}

# resource "google_compute_instance" "compute_instance_actual_earwig" {
#   name         = "actual-earwig"
#   machine_type = "f1-micro"

#   boot_disk {
#     initialize_params {
#       image = "debian-cloud/debian-11"
#     }
#   }

#   network_interface {
#     network = google_compute_network.compute_instance_actual_earwig.name
#   }

#   metadata = {
#     block-project-ssh-keys = false
#   }

#   labels = {
#     name = "actual-earwig"
#   }
# }

output "compute_instance_actual_earwig" {
  value = google_compute_instance.compute_instance_actual_earwig.id
}
