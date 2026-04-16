# Propuesta de Desarrollo y Entrega

## App Local GN a Tienda Nube

### Resumen

Se desarrollará una aplicación local para Windows que permita consultar la API de Grupo Núcleo, transformar los productos al formato de Tienda Nube y generar archivos listos para importar o actualizar productos de forma masiva.

La propuesta contempla una solución simple de usar, sin hosting ni servidor, con configuración editable, accesos directos por doble click y acompañamiento posterior a la entrega.

### Alcance del Producto Final

La entrega incluirá:

- App local para consultar catálogo, precios y stock desde Grupo Núcleo.
- Exportación de productos en CSV y Excel compatibles con Tienda Nube.
- Posibilidad de actualizar productos existentes mediante importación del mismo archivo.
- Configuración editable para reglas comerciales como margen, dólar manual, publicación, redondeo y textos.
- Comparación entre exportaciones para detectar cambios de stock y precio.
- Exportación del catálogo "en crudo" de GN para control interno.
- Flujo de prueba limitado para validar resultados antes de una carga completa.
- Accesos directos `.bat` para operar la app de forma simple.
- Documentación de uso, configuración y operación.

### Forma de Trabajo

El desarrollo se organizará en etapas cortas para validar cada parte importante antes de cerrar el proyecto:

1. Definición funcional y reglas comerciales.
2. Integración con la API de Grupo Núcleo.
3. Transformación de datos y generación de archivos para Tienda Nube.
4. Validación con pruebas reales de importación y ajuste fino de formato.
5. Entrega final, documentación y puesta en marcha.

Durante el desarrollo se irán revisando juntos los avances para asegurar que el resultado final quede alineado con la operatoria real del negocio.

### Entrega y Validación

El proyecto se considerará entregado cuando:

- La app pueda conectarse correctamente a la API.
- Los productos se exporten con el formato acordado para Tienda Nube.
- Los precios y stocks salgan según la configuración definida.
- Existan opciones simples para exportar, probar, comparar y revisar datos crudos.
- El cliente pueda ejecutar el flujo principal de forma local mediante `.bat`.

### Soporte

Durante los primeros 60 días desde la entrega final, el soporte será completo y no se cobrará ningún cambio que sea necesario realizar sobre la aplicación.

Pasado ese plazo, cualquier ajuste, mejora o modificación adicional se evaluará por separado según el alcance de lo que se necesite.

### Tiempo Estimado e Inversión

El desarrollo completo se estima en aproximadamente 12 a 20 horas de trabajo.

La inversión total propuesta para el proyecto es de **USD 450**, incluyendo el desarrollo, la puesta en marcha y los primeros 60 días de soporte completo.

### Supuestos y Condiciones Base

- La solución será una app local para Windows.
- No incluye hosting, servidor ni infraestructura externa.
- No incluye interfaz gráfica propia ni versión `.exe` en esta etapa.
- El uso principal será generar archivos para importar o actualizar productos en Tienda Nube.
- Las credenciales de la API serán provistas por el cliente.
- Luego del período inicial de soporte, cualquier nueva necesidad se cotizará o definirá aparte.
