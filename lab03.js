// José Gerardo Ruiz García - 23719
// Relación existente: Un cliente tiene un muchas cuentas y una cuenta tiene muchas transacciones.

use "sample_analytics"

db.accounts.find();
db.customers.find();
db.transactions.find();

// 2.1 Calcule para cada cliente el número total de transacciones y el 
// monto promedio de cada una. Muestre el nombre completo, ciudad y 
// los resultados ordenados por número de transacciones descendente.
db.customers.aggregate([

  {
    $lookup: {
      from: "accounts", // Ve a esta colección 
      localField: "accounts", // Usa este campo de la colección actual
      foreignField: "account_id", // Campo de la colecciónd definida en from
      as: "accountData"
    }
  },

  {
    $lookup: {
      from: "transactions",
      localField: "accountData.account_id",
      foreignField: "account_id",
      as: "transactionBuckets"
    }
  },

  { $unwind: "$transactionBuckets" },
  { $unwind: "$transactionBuckets.transactions" },

  {
    $group: {
      _id: "$_id",
      name: { $first: "$name" },
      address: { $first: "$address" },
      totalTransactions: { $sum: 1 },
      avgAmount: {
        $avg: "$transactionBuckets.transactions.amount"
      }
    }
  },

  {
    $addFields: {
      city: {
        $arrayElemAt: [
          { $split: ["$address", "\n"] },
          1
        ]
      }
    }
  },

  { $sort: { totalTransactions: -1 } }

])

// 2.2 Clasifique a los clientes en tres categorías según su balance total
db.customers.aggregate([

  {
    $lookup: {
      from: "accounts",
      localField: "accounts",
      foreignField: "account_id",
      as: "accountData"
    }
  },

  {
    $addFields: {
      totalBalance: { $sum: "$accountData.limit" }
    }
  },

  {
    $addFields: {
      category: {
        $switch: {
          branches: [
            { case: { $lt: ["$totalBalance", 5000] }, then: "Bajo" },
            { 
              case: { 
                $and: [
                  { $gte: ["$totalBalance", 5000] },
                  { $lte: ["$totalBalance", 20000] }
                ]
              }, 
              then: "Medio" 
            },
            { case: { $gt: ["$totalBalance", 20000] }, then: "Alto" }
          ],
          default: "Sin categoría"
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      name: 1,
      category: 1
    }
  }

])

// 2.3 Para cada ciudad, identifique el cliente con el mayor balance total sumando todas sus cuentas
db.customers.aggregate([

  {
    $lookup: {
      from: "accounts",
      localField: "accounts",
      foreignField: "account_id",
      as: "accountData"
    }
  },

  { $unwind: "$accountData" },

  {
    $group: {
      _id: "$_id",
      name: { $first: "$name" },
      address: { $first: "$address" },
      totalBalance: { $sum: "$accountData.limit" }
    }
  },

  {
    $addFields: {
      city: {
        $arrayElemAt: [
          { $split: ["$address", "\n"] },
          1
        ]
      }
    }
  },

  {
    $sort: { city: 1, totalBalance: -1 }
  },

  {
    $group: {
      _id: "$city",
      name: { $first: "$name" },
      totalBalance: { $first: "$totalBalance" }
    }
  },

  {
    $project: {
      _id: 0,
      name: 1,
      city: "$_id",
      totalBalance: 1
    }
  }

])

// 2.4 Extraiga las 10 transacciones más altas realizadas en los últimos 6 meses 
// y enriquezca el resultado con información del cliente asociado
db.transactions.aggregate([
    
  { $unwind: "$transactions" },

  {
    $match: {
      "transactions.date": {
        $gte: {
          $dateSubtract: {
            startDate: "$$NOW",
            unit: "month",
            amount: 6
          }
        }
      }
    }
  },

  {
    $sort: {
      "transactions.amount": -1
    }
  },

  { $limit: 10 },

  {
    $lookup: {
      from: "customers",
      localField: "account_id",
      foreignField: "accounts",
      as: "customerData"
    }
  },

  { $unwind: "$customerData" },

  {
    $project: {
      _id: 0,
      customerName: "$customerData.name",
      email: "$customerData.email",
      account_id: 1,
      amount: "$transactions.amount",
      date: "$transactions.date"
    }
  }

])


// 2.5 Para cada cliente, calcule la variación porcentual entre su 
// transacción más reciente y la más antigua. Solo incluya clientes con al menos dos transacciones
db.transactions.aggregate([
  {
    $project: {
      account_id: 1,
      transaccionesOrdenadas: {
        $sortArray: {
          input: "$transactions",
          sortBy: { date: 1 }
        }
      }
    }
  },

  {
    $project: {
      account_id: 1,
      total: { $size: "$transaccionesOrdenadas" },
      primera: { $arrayElemAt: ["$transaccionesOrdenadas", 0] },
      ultima: {
        $arrayElemAt: [
          "$transaccionesOrdenadas",
          { $subtract: [{ $size: "$transaccionesOrdenadas" }, 1] }
        ]
      }
    }
  },

  {
    $match: { total: { $gte: 2 } }
  },

  {
    $addFields: {
      variacionPorcentual: {
        $cond: [
          { $eq: ["$primera.amount", 0] },
          null,
          {
            $round: [
              {
                $multiply: [
                  {
                    $divide: [
                      { $subtract: ["$ultima.amount", "$primera.amount"] },
                      "$primera.amount"
                    ]
                  },
                  100
                ]
              },
              2
            ]
          }
        ]
      }
    }
  },

  {
    $project: {
      _id: 0,
      account_id: 1,
      variacionPorcentual: 1
    }
  }

])

// 2.6 Agrupe las transacciones por mes y tipo de transacción.
// Calcule el total y el promedio por grupo.
db.transactions.aggregate([
    
  { $unwind: "$transactions" },

  {
    $group: {
      _id: {
        year: { $year: "$transactions.date" },
        month: { $month: "$transactions.date" },
        tipo: "$transactions.transaction_code"
      },
      totalMonto: { $sum: "$transactions.amount" },
      promedioMonto: { $avg: "$transactions.amount" },
      cantidad: { $sum: 1 }
    }
  },

  {
    $project: {
      _id: 0,
      year: "$_id.year",
      month: "$_id.month",
      tipo: "$_id.tipo",
      totalMonto: 1,
      promedioMonto: { $round: ["$promedioMonto", 2] },
      cantidad: 1
    }
  }

])

// 2.7 Identifique clientes que no han realizado ninguna 
// transacción y guárdelos en una nueva colección llamada inactive_customers.
db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transactionData"
    }
  },

  {
    $match: {
      transactionData: { $eq: [] }
    }
  },

  {
    $project: {
      _id: 1,
      name: 1,
      email: 1,
      accounts: 1
    }
  },

  {
    $merge: {
      into: "inactive_customers",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }

])

// 2.8 Para cada tipo de cuenta, actualice un documento resumen en 
// una colección account_summaries que contenga el total de cuentas 
// y el balance promedio por tipo
db.accounts.aggregate([

  { $unwind: "$products" },

  {
    $group: {
      _id: "$products",
      totalAccounts: { $sum: 1 },
      averageBalance: { $avg: "$limit" }
    }
  },

  {
    $sort: { _id: 1 }
  },

  {
    $project: {
      _id: 0,
      accountType: "$_id",
      totalAccounts: 1,
      averageBalance: { $round: ["$averageBalance", 2] },
      lastUpdated: "$$NOW"
    }
  },

  {
    $out: "account_summaries"
  }

])

db.account_summaries.find();

// 2.9 Cree una colección high_value_customers que contenga 
// los clientes cuyo balance total sea mayor a 30,000 y que 
// hayan realizado más de 5 transacciones. Incluya su nombre, email, 
// total_balance y número de transacciones.
db.customers.aggregate([
  {
    $lookup: {
      from: "accounts",
      localField: "accounts",
      foreignField: "account_id",
      as: "accountData"
    }
  },

  {
    $addFields: {
      total_balance: { $sum: "$accountData.limit" }
    }
  },

  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transactionData"
    }
  },

  {
    $addFields: {
      total_transactions: { $sum: "$transactionData.transaction_count" }
    }
  },

  {
    $match: {
      total_balance: { $gt: 30000 },
      total_transactions: { $gt: 5 }
    }
  },

  {
    $project: {
      _id: 0,
      name: 1,
      email: 1,
      total_balance: 1,
      total_transactions: 1
    }
  },

  {
    $out: "high_value_customers"
  }

])

db.high_value_customers.find();

// 2.10 Para cada cliente, calcule el promedio mensual de transacciones en el último año y clasifíquelo
db.transactions.aggregate([
  { $unwind: "$transactions" },

  {
    $group: {
      _id: null,
      maxDate: { $max: "$transactions.date" }
    }
  },

  {
    $project: {
      maxDate: 1,
      cutoffDate: {
        $dateSubtract: {
          startDate: "$maxDate",
          unit: "year",
          amount: 1
        }
      }
    }
  },

  {
    $lookup: {
      from: "transactions",
      pipeline: [
        { $unwind: "$transactions" }
      ],
      as: "allData"
    }
  },

  { $unwind: "$allData" },

  {
    $match: {
      $expr: {
        $gte: [
          "$allData.transactions.date",
          "$cutoffDate"
        ]
      }
    }
  },

  {
    $lookup: {
      from: "customers",
      localField: "allData.account_id",
      foreignField: "accounts",
      as: "customer"
    }
  },

  { $unwind: "$customer" },

  {
    $group: {
      _id: {
        customerId: "$customer._id",
        year: { $year: "$allData.transactions.date" },
        month: { $month: "$allData.transactions.date" }
      },
      name: { $first: "$customer.name" },
      totalMes: { $sum: 1 }
    }
  },

  {
    $group: {
      _id: "$_id.customerId",
      name: { $first: "$name" },
      promedioMensual: { $avg: "$totalMes" }
    }
  },

  {
    $addFields: {
      promedioMensual: { $round: ["$promedioMensual", 1] },
      categoria: {
        $switch: {
          branches: [
            { case: { $lt: ["$promedioMensual", 2] }, then: "infrequent" },
            {
              case: {
                $and: [
                  { $gte: ["$promedioMensual", 2] },
                  { $lte: ["$promedioMensual", 5] }
                ]
              },
              then: "regular"
            },
            { case: { $gt: ["$promedioMensual", 5] }, then: "frequent" }
          ],
          default: "infrequent"
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      name: 1,
      promedioMensual: 1,
      categoria: 1
    }
  }

])
