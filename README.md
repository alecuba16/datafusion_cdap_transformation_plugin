# datafusion_cdap_transformation_plugin
Complete example project to create a custom Google cloud datafusion (CDAP) transformation plugin. Sourced and adapted from the documentation where there is no quickstart project.

## Transformation Plugin
A Transform plugin is used to convert one input record into zero or more output records. It can be used in both batch and real-time data pipelines.

The only method that needs to be implemented is: **transform()**

## Methods
### initialize():
Used to perform any initialization step that might be required during the runtime of the Transform. It is guaranteed that this method will be invoked before the transform method.

### transform(): 
This method contains the logic that will be applied on each incoming data object. An emitter can be used to pass the results to the subsequent stage.

### destroy():
Used to perform any cleanup before the plugin shuts down.