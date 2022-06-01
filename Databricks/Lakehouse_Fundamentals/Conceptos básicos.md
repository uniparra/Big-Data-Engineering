<h2>LAKEHOUSE</h2> Un data Lakehouse nace de la necesidad de aligerar los procesos necesarios desde ETL hasta el BI o el ML. Para ello combina los beneficios en el manejo del dato de los data warehouse directamente sobre los sistemas de bajo coste de los data lakes. Unificando así en una sola plataforma todo el procesado del dato hasta el analista de turno (ya sea ML o BI).
<h4><span style="color:Grey">Características clave</span></h4> 
<ul>
  <li>Soporte en las transacciones para asegurar que las múltiples partes pueden concurrir en la lectura o la escritura del dato. </li>
  <li>Cumplimiento del esquema de datos para garantizar la integridad de los datos (las escrituras en una tabla se rechazan si no coinciden con el esquema de la tabla).</li>
  <li>Mecanismos de governanza y auditoría para asegurar que puedas ver como se está usando el dato.</li>
  <li>Compatibilidad con BI para que las herramientas de BI puedan trabajar directamente en los datos de origen; esto reduce la obsolescencia de los datos.</li>
  <li>El almacenamiento está separado del cómputo, lo que significa que es más fácil para el sistema escalar a más usuarios simultáneos y tamaño de datos.</li>
  <li> Los formatos (Openness - Storage) utilizados son abiertos y estándar. Además, las API y otras herramientas facilitan que los miembros del equipo accedan a los datos directamente. </li>
  <li>Soporta cualquier tipo de datos; estructurados, semi estructurados y desestructurado</li>
  <li>Transmisión de extremo a extremo para que los informes en tiempo real y los datos en tiempo real puedan integrarse en los procesos de análisis de datos tal como lo están los datos existentes.</li>
  <li>Compatibilidad con diversas cargas de trabajo, incluida la ingeniería de datos, la ciencia de datos, el aprendizaje automático y el análisis de SQL, todo en el mismo repositorio de datos.</li>
  </ul>
  
  <h4><span style="color:Gray">Componentes del Lakehouse</span></h4>
  Un Lakehouse es el producto de unir un data lake abierto y una capa open source, de almacenamiento que aporta fiabilidad a los datos llamada, Delta Lake. Cuando se dice fiabilidad, se refiere a la precisión e integridad.
 <h4><span style="color:Gray">Databrics Lakehouse para la ingeniería de datos: Delta Lake</span></h4>
 <h6><span style="color:Orange">Delta Lake </span></h6>
  Se garantiza que el dato sea lo que se necesite según el caso de uso, mediante: 
  <ul>
  <li><span style="color:Green">Transacciones ACID</span></li>
  <li><span style="color:Green">Indexación</span>, que le permite obtener una tabla desordenada (que podría ser ineficiente para consultar) en un orden que maximizará la eficiencia de sus consultas.</li>
  <li><span style="color:Green">Listas de control de acceso a tablas (ACL)</span> o mecanismos de gobierno que garantizan que solo los usuarios que tengan permitido tener acceso a los datos puedan acceder a ellos.</li>
  <li><span style="color:Green">Establecimiento de expectativas</span>, es decir, la capacidad de configurar Delta Lake en función de sus patrones de carga de trabajo y necesidades comerciales.</li>
  </ul>
Con <span style="color:Green">estas características</span> un ingeniero de datos puede diseñar pipelines para flujos de datos continuos y refinamiento para los mismos utilizando el modelo Delta. De esta manera, permite a los ingenieros de datos construir pipelines que comienzan con datos sin procesar como una "única fuente de verdad" desde la cual todo fluye. Las transformaciones y agregaciones posteriores se pueden recalcular y validar para garantizar que las tablas agregadas de nivel empresarial sigan reflejando los datos subyacentes, incluso cuando los usuarios intermedios refinan los datos e introducen una estructura específica del contexto.


<h4><span style="color:Gray">Databrics Lakehouse para BI y Analítica SQL: Databrics SQL</span></h4>

Databricks SQL proporciona a los usuarios de SQL una interfaz nativa de SQL para escribir consultas que exploran la tabla Delta Lake de su organización. El código SQL que se usa regularmente se puede guardar como fragmentos para una reutilización rápida, y los resultados de la consulta se pueden almacenar en caché para mantener la consulta breve.

Una vez que se crean las consultas, Databricks SQL también permite a los analistas dar sentido a los resultados a través de una amplia variedad de visualizaciones. Las visualizaciones se pueden organizar en paneles a través de una interfaz de arrastrar y soltar. Los paneles se pueden compartir fácilmente con otros usuarios para proporcionar información. También se pueden configurar para actualizar automáticamente y alertar a su equipo sobre cambios significativos en sus datos.

Dado que Databricks SQL se diseñó para funcionar con Delta Lake, los profesionales obtienen las optimizaciones de rendimiento, la gobernanza y la confiabilidad que conlleva.

Además, es compatible con las herramientas de BI existentes, lo que significa que puede usar sus herramientas de visualización preferidas para consultar el Lakehouse de su organización.

<h4><span style="color:Gray">Databrics Lakehouse para Data science y ML: Databricks Machine Learning</span></h4>
Databricks Machine Learning (Databricks ML) se creó para que los equipos de aprendizaje automático exploren datos, preparen y procesen datos, creen y prueben modelos de aprendizaje automático, implementen esos modelos y los optimicen.
 <h6>Commponentes de Databrics ML</h6>
 <ul>
  <li><span style="color:Green">Databrics Collaborative Notebooks</span> es una interface web que contiene código ejecutable, opciones de visualización y texto narrativo. Admite múltiples lenguajes de programación (SQL, Scla, R, Python y Java) visualizaciones de datos integradas, versiones automáticas y la capacidad de automatizar procesos.</li>
  <li><span style="color:Green">Databrics Machine Learning Runtime (MLR)</span>proporciona a los científicos de datos y profesionales de ML recursos informáticos escalables que vienen con marcos de ciencia de datos populares incorporados. Proporciona un entorno informáticco optimizado para flujos de trabaj de aprendizaje automático.</li>
  <li><span style="color:Green">Feature Store</span> brinda a los equipos de datos la capacidad de optimizar su trabajo al crear nuevas funciones para usar en sus modelos de aprendizaje automático, explorar y reutilizar funciones existentes y crear conjuntos de datos de capacitación.</li>
  <li><span style="color:Green">Auto ML</span> realiza un seguimiento rápido del flujo de trabajo de aprendizaje automático al proporcionar código de capacitación para ejecuciones de prueba de modelos: los científicos de datos pueden usar este código para evaluar rápidamente la viabilidad de usar un conjunto de datos para el aprendizaje automático o para obtener una verificación rápida de la cordura en la dirección de un proyecto de ML.</li>
  <li><span style="color:Green">Managed MLflow</span> se basa en MLflow, una plataforma de código abierto desarrollada por Databricks para ayudar a administrar el ciclo de vida completo del aprendizaje automático. Con MLflow, puede realizar un seguimiento de los experimentos de aprendizaje automático, administrar modelos de aprendizaje automático e implementar esos modelos.</li>
  <li><span style="color:Green">Model Serving</span>Implementación con un solo clic de cualquier modelo de ML como un punto final REST para un servicio de baja latencia. Se integra con Model Registry para administrar las versiones de prueba y producción de los puntos finales.</li>
  </ul>
  
  <h4><span style="color:Grey">Seguridad de la plataforma</span></h4>
  Databrics proporciona varias herramientas para la protección de sus infraestructuras en la red y datos. ¿COMO?
  <ul>
  <li>Manteniendo los datos seguros a medida que una organización crece. La mayor diferencia entre Databrics y otras plataformas es que la organizaciones siempre tienen el control de sus datos; el dato queda almacenado en reposo en la propia cuenta cloud de la empresa u organización.</li>
    <ul>
      <li>Utilice proveedores de identidades nativos de la nube que admitan el protocolo SAML para autenticar a sus usuarios.</li>
      <li>Utilice sus proveedores de identidad existentes para definir políticas de acceso para todos los datos en su lago de datos</li>
      <li>Federar la identidad enntre sus proveedores de identidad y Databrics para garantizar un acceso seguro a los datos en el almacenamineto de objetos</li>
      <li>Conservar el control total sobre las claves utilizadas para cifrar sus datos (hasta el nivel de cuaderno individual)</li>
      <li>Establezca la jerarquía de claves para que la revocación de una clave corte el acceso a los datos.</li>
    </ul>
  <li>Garantizar la privacidad a medida que los profesionales de datos llevan a cabo sus flujos de trabajo. Muchas veces, es necesario brindar transparencia en los proyectos, especialmente en los proyectos en los que se requiere colaboración y retroalimentación en tiempo real. Otras veces, los profesionales de datos que trabajan en proyectos necesitan cosas bloqueadas para garantizar la privacidad de datos/proyectos.</li>
  <ul>
    <li>VPC/VNET administradas por el cliente: estas son redes privadas que se pueden implementar con configuraciones de red personalizadas para cumplir con las políticas internas de gobierno de datos y de la nube (además de cumplir con las regulaciones externas).</li>
    <li>Listas de control de acceso/listas blancas de IP: esto permite el uso de listas de acceso de IP estrictas para especificar qué conexiones pueden o no hacerse dentro y fuera de los espacios de trabajo de una organización, minimizando la superficie de ataque. Con listas de control de acceso/listas blancas de IP, todos los accesos entrantes a la aplicación web de Databricks de una organización y las API REST requieren que los usuarios se conecten desde una dirección IP o VPN autorizada.</li>
    <li>
Aislamiento de código: esto significa que los profesionales de datos pueden ejecutar sus flujos de trabajo de análisis de datos en los mismos recursos computacionales mientras se aseguran de que cada usuario solo tenga acceso a los datos a los que está autorizado a acceder.</li>
    <li>Red privada entre datos y planos de control: la plataforma Lakehouse de Databricks está estructurada para permitir la colaboración segura entre equipos multifuncionales mientras mantiene una cantidad significativa de servicios de back-end administrados por Databricks. Databricks opera desde un plano de control y un plano de datos. El plano de control incluye los servicios de backend que Databricks administra en su propia cuenta en la nube. El plano de datos es administrado por la cuenta en la nube de una organización y es donde residen y se procesan los datos. Con las redes privadas entre los planos de datos y de control, todas las comunicaciones entre los planos de control y de datos se realizan a través de redes privadas (no se envían a través de redes públicas).</li>
  </ul>
  <li>Cumplir con las normas de cumplimiento y certificación. Databricks incorpora las mejores prácticas líderes en la industria en su programa de seguridad y emplea firmas de CPA independientes registradas en la PCAOB para auditar regularmente el programa y dar fe de sus certificaciones. Además, se han implementado una serie de controles para satisfacer las necesidades únicas de cumplimiento de las industrias altamente reguladas.</li>
  <ul>
    <li>SOC 2 Type II</li>
    <li>ISO 27018</li>
    <li>ISO 27001</li>
    <li>HIPAA</li>
    <li>GDPR|Read our FAQ</li>
    <li>FedRAMP(Azure)</li>
    <li>PCI DSS(AWS)</li>
  </ul>
  </ul>
