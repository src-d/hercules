Hercules plugins
================

Generate a new plugin skeleton:

```
hercules-generate-plugin -n MyPluginName -o my_plugin
```

Compile:

```
cd my_plugin
make
```

Use:

```
hercules -plugin my_plugin_name.so -my-plugin-name https://github.com/user/repo
```