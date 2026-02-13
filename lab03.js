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

// Clasifique a los clientes en tres categorías según su balance total
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

// Para cada ciudad, identifique el cliente con el mayor balance total sumando todas sus cuentas
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


