This is a fork of the orignal open-dc traces repository to enable creating synthetic workflow traces. This repository also contains information regarding all traces used by the [OpenDC](https://github.com/atlarge-research/opendc) framework. In this repository, users can find many traces used in previous works, and scripts to generate new traces. 

Three types of traces are used:

- *Failure Traces:* are used to inject failures during OpenDC simulations. 

- *Carbon Traces:* are used to calculate the Carbon Emissions of data centers

- *Workload Traces:* Define the tasks OpenDC should execute, and when. 

For more information about OpenDC look [here](https://github.com/atlarge-research/opendc). For OpenDC documentation see [this](https://atlarge-research.github.io/opendc/docs/category/documentation).

## Setup
Very raw setup!

```bash
python -m venv myenv
source myenv/bin/activate

pip install -r requirements.txt
```

## How to use

Simply run the `./workload/generate_synthetic_workload_examples.py` script!
