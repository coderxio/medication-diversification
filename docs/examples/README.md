# Examples Usage

This directory contains example module directories which can be used with MDT to build Synthea modules. Each directory contains a single `settings.yaml` which MDT will read to build the appropriate Synthea module and related files. After installing MDT with pip and building the database with `mdt init` execute `mdt module -n rescue_inhaler build`, for example, to build the rescue_inhaler submodule.

End users can either copy the example module directory to the same location as where MDT was initialized or copy the settings file into a custom directory use the same steps in the repo README to build the module. 
