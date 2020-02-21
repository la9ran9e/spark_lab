## Spark pipeline

This is an example project on Scheduler demonstrating abilities of this library and some Spark features.

Job represents a pipeline containing 3 tasks: extracting data, converting to appropriate format,
transforming and loading this.

Data is a massive of JSON-formatted cooking recipes. We need extract it from source, 
retrieve all recipes containing meat, estimate cooking complexity and load result to disc.

Pipeline starts every minute at 10th second.

### Content:
* [Source code of job](jobs/meat_recipes.py)
* [Scheduler application source code](app.py)

### Build

Before running app prepare `.env` file in project root directory with next environment variables:

```bash
RECIPES_URL  # required
WORKDIR  # default /tmp 
```

### Run

To run application call next command:

```bash
python3.6 app.py
```