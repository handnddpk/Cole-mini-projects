db = db.getSiblingDB('admin');

// Create admin user if it doesn't exist
if (db.getUser("admin") == null) {
    db.createUser({
        user: "admin",
        pwd: "admin123",
        roles: ["root"]
    });
}

// Switch to product_catalog database
db = db.getSiblingDB('product_catalog');

// Create application user
db.createUser({
    user: "app_user",
    pwd: "app_password",
    roles: [
        { role: "readWrite", db: "product_catalog" }
    ]
});

// Create products collection and insert sample data
db.products.insertMany([
    {
        id: 1,
        name: "Laptop Pro X1",
        category: "Electronics",
        price: 1299.99,
        stock: 45,
        description: "High-performance laptop with 16GB RAM and 512GB SSD"
    },
    {
        id: 2,
        name: "Smartphone Y2",
        category: "Electronics",
        price: 799.99,
        stock: 120,
        description: "Latest smartphone with 128GB storage and 5G capability"
    },
    {
        id: 3,
        name: "Wireless Headphones Z3",
        category: "Audio",
        price: 199.99,
        stock: 75,
        description: "Noise-cancelling wireless headphones with 30-hour battery life"
    },
    {
        id: 4,
        name: "Coffee Maker Deluxe",
        category: "Kitchen",
        price: 129.99,
        stock: 30,
        description: "Programmable coffee maker with thermal carafe"
    },
    {
        id: 5,
        name: "Fitness Tracker Pro",
        category: "Wearables",
        price: 149.99,
        stock: 60,
        description: "Advanced fitness tracker with heart rate monitoring and GPS"
    },
    {
        id: 6,
        name: "Smart Home Hub",
        category: "Smart Home",
        price: 179.99,
        stock: 25,
        description: "Central hub for controlling all your smart home devices"
    },
    {
        id: 7,
        name: "Wireless Charging Pad",
        category: "Accessories",
        price: 49.99,
        stock: 100,
        description: "Fast wireless charging pad compatible with all Qi-enabled devices"
    },
    {
        id: 8,
        name: "Bluetooth Speaker",
        category: "Audio",
        price: 89.99,
        stock: 50,
        description: "Portable Bluetooth speaker with 12-hour battery life"
    },
    {
        id: 9,
        name: "Digital Camera",
        category: "Photography",
        price: 599.99,
        stock: 15,
        description: "24MP digital camera with 4K video recording"
    },
    {
        id: 10,
        name: "Gaming Console",
        category: "Gaming",
        price: 499.99,
        stock: 20,
        description: "Next-gen gaming console with 1TB storage"
    }
]);