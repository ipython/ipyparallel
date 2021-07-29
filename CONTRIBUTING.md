# Contributing

We follow the [Jupyter Contributing Guide](https://jupyter.readthedocs.io/en/latest/contributing/content-contributor.html).

Make sure to follow the [Jupyter Code of Conduct](https://jupyter.org/conduct/).

## Development install

A basic development install of IPython parallel
is the same as almost all Python packages:

```bash
pip install -e .
```

To enable the server extension from a development install:

```bash
# for jupyterlab
jupyter server extension enable --sys-prefix ipyparallel
# for classic notebook
jupyter serverextension enable --sys-prefix ipyparallel
```

As described in the [JupyterLab documentation](https://jupyterlab.readthedocs.io/en/stable/extension/extension_dev.html#developing-a-prebuilt-extension)
for a development install of the jupyterlab extension you can run the following in this directory:

```bash
jlpm  # Install npm package dependencies
jlpm build  # Compile the TypeScript sources to Javascript
jupyter labextension develop . --overwrite  # Install the current directory as an extension
```

If you are working on the lab extension,
you can run jupyterlab in dev mode to always rebuild and reload your extensions.
In two terminals, run:

```bash
[term 1] $ jlpm watch
[term 2] $ jupyter lab --extensions-in-dev-mode
```

You should then be able to refresh the JupyterLab page
and it will pick up the changes to the extension as you work.
