// orderRouter.js
const express = require('express');
const router = express.Router();
const {
  createOrder,
  getOrders,
  getOrderById,
  updateOrder,
  deleteOrder,
  updateOrderStatus,
  getAllOrders,
  getOrdersBySeller,
} = require('../controllers/orderController');
const authenticateToken = require('../middleware/authMiddleware');
const validateRole = require('../middleware/validateRole');


// Create an order
router.post('/', authenticateToken, validateRole(['USER']), createOrder);

// Get all orders for the user
router.get('/', authenticateToken, validateRole(['USER']), getOrders);


// Add this route above the module.exports
router.get('/seller/orders', authenticateToken, validateRole(['SHOP_OWNER']), getOrdersBySeller);

// Get all orders (Admin only)
router.get('/all', authenticateToken, validateRole(['ADMIN']), getAllOrders);

// Get an order by ID
router.get('/:id', authenticateToken, validateRole(['USER']), getOrderById);

// Update an order (for Users)
router.put('/:id', authenticateToken, validateRole(['USER']), updateOrder);

// Delete an order (for Users)
router.delete('/:id', authenticateToken, validateRole(['USER']), deleteOrder);


// Update order status (Admin only)
router.patch('/:id', authenticateToken, validateRole(['ADMIN']), updateOrderStatus);



module.exports = router;