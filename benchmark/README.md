# Running autobuhn on DOM-BFT GClound setup

## Setup settings.json

### GCP Config <a name="gcpconfig"></a>
The GCP config that is used is found in `autobahn-artifact/benchmark/settings.json`. We provided examples for Autobahn/Bullshark config in `autobahn-artifact/benchmark/settings-autobahn-bullshark.json` and Vanilla HotStuff/Batched HotStuff in `autobahn-artifact/benchmark/settings-hotstuff-baselines.json`. You will need to change the following:
1. `key`: change the `name` (name of the private SSH key) and `path` (full path including private key file name) fields to match the key you generated in the prior section.
Leave `port` unchanged (should be `5000`).
For HotStuff/Batched HotStuff leave `ports` unchanged.

2. `repo`: Leave `name` unchanged (should be `autobahn-artifact`). Leave url unchanged (should be `https://github.com/neilgiri/autobahn-artifact.git`). `branch` specifies which branch will be run on all the machines. This will determine which system ends up running. Only select an Autobahn or Bullshark branch if you are in the `autobahn-bullshark` folder. Similarly, only select a Vanilla HotStuff or Batched HotStuff branch if you are in the `hotstuff-baselines` folder.

3. `project_id`: the project id is found by clicking the dropdown of your project (e.g. "My First Project") on the top left side, and looking at the ID field.

4. `instances`: `type` (value of t2d-standard-16; t2d-standard-4 if using free trial) and `regions` (value of ["us-east1-b", "us-east5-a", "us-west1-b", "us-west4-a"]) should remain unchanged. If you select different regions then you will need to change the regions field to be the regions you are running in. You will need to change `templates` to be the names of the instance templates you created. The order matters, as they should correspond to the order of each region. The path should be in the format "projects/PROJECT_ID/regions/REGION_ID/instanceTemplates/TEMPLATE_ID", where PROJECT_ID is the id of the project you created in the prior section, REGION_ID is the name of the region without the subzone (i.e. us-east1 NOT us-east1-a).

5. `username`: This is the username of the ssh key you generated for the control machine. You can find it in the metadata console if you forgot.

## Control machine setup

An install script `install_deps.sh` is provided in the `overview` branch that automatically installs the required dependencies.

After installation finishes, navigate to `autobahn-artifact/benchmark` and run `pip install -r requirements.txt`.

Note: Autobuhn uses a gcloud machine as the control machine, but in DOM-BFT we use local machine as control machine, so the corresponding scripts as modified for using the same workflow used in DOM-BFT...

## Setup gcloud machines

Since we already have nodes created on gcloud, we only need to install the env used by the proj. 

Run `fab install` which will install rust and the dependencies on these machines.

Note: the script fetches the info of all machines in the `project_id` that are running. So no configuration on which machine takes which role..

## Run exps

Run `fab remote` which will launch a remote experiment with the parameters specified in `fabfile.py`.

> [!NOTE] 
> To reproduce a specific experiment simply copy a config of choice from `experiment_configs` into the `fabfile.py` `remote` task. 

### More

See [original README](https://github.com/Hoooao/autobahn-artifact/blob/overview/README.md#configuring-parameters)