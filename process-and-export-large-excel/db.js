const { Sequelize } = require("sequelize");

const db = new Sequelize(
  "postgres://postgres:Welcome_123@assests.c2dvmpnh8nmc.ap-south-1.rds.amazonaws.com:5432/assets",
  {
    dialect: "postgres",
    logging: false,
    protocol: "postgres",
    dialectOptions: {
      ssl: {
        require: true,
        rejectUnauthorized: false,
      },
    },
  }
);

const connectDatabase = async () => {
  try {
    await db.authenticate();
    console.log("Connected To database!");
  } catch (error) {
    throw new Error("Failed To ");
  }
};

module.exports = {
  db,
  connectDatabase,
};
