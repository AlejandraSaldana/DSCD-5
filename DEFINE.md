# Casuística A2 — Estado de entrega consolidado de pedidos

## 1. Fuentes

1) SQLite: orders
Incluye la información de las ordenes, junto con su estado.
Tiene un relación 1:N con shipments, dado que una misma orden puede tener varios shipments. 

2) CSV: shipments
Incluye la información de cada shipment, junto con su fecha de envío y de ser el caso, entrega. 

3) JSON: carriers
Son los diferentes transportistas.
Un mismo carrier puede estar en varios shipments, pero un shipment solo tiene un carrier.

## 2.  Grain
Es un pedido específico, por lo que cada fila de orders_curated representa una orden.

## 3. Business Key
`order_id`
Identifica a cada orden de manera única.

## 4. Refresh Strategy
La Refresh Strategy será incremental, ya que al tener `updated_at` en la table de `orders`, en cada ejecución se pueden procesar solo las órdenes que se actualizaron desde la ejecución pasada, evitando procesar todos los datos de nuevo.

## 5. Contrato de datos (orders_curated)

- order_id (INTEGER, obligatorio): existe en orders, único por fila, es el grain.
- shipping_status (TEXT, obligatorio): "delivered" o "in_transit". Se saca del shipment más lento del pedido: si ese no tiene delivered_at, va "in_transit"; si sí, "delivered".
- delivery_delay_days (INTEGER, opcional): shipped_at menos delivered_at del shipment más lento, en días. Solo puede estar vacío si el status es "in_transit". 
- carrier_name (TEXT, obligatorio): se saca el nombre a partir del carrier_code del shipment más lento, usando carriers.json. Si el código no está , se pone "desconocido" y se cuenta en unknown_carrier_rate.

## 6. Incumplimiento de contrato
Si un registro no cumple con las reglas de validación, se elimina. Se usará una tabla de cuarentena para mandarlos junto con la razón de rechazo. 
