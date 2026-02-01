/**
 * Recalculates invoice balance and status based on all paid payments.
 * This is the single source of truth for invoice balance calculations.
 *
 * @param {Object} supabase - Supabase client instance
 * @param {number} invoiceId - The ID of the invoice to recalculate
 * @returns {Promise<void>}
 */
async function recalculateInvoiceBalance(supabase, invoiceId) {
  try {
    // 1. Get all paid payments for this invoice
    const { data: paidPayments, error: paymentsError } = await supabase
      .from("payments")
      .select("amount")
      .eq("invoice_id", invoiceId)
      .eq("status", "Paid");

    if (paymentsError) {
      throw new Error(`Failed to fetch payments: ${paymentsError.message}`);
    }

    const totalPaid = (paidPayments || []).reduce(
      (sum, p) => sum + (p.amount || 0),
      0,
    );

    // 2. Get invoice details
    const { data: invoice, error: invoiceError } = await supabase
      .from("invoices")
      .select("total_amount, due_date, status")
      .eq("id", invoiceId)
      .single();

    if (invoiceError) {
      throw new Error(`Failed to fetch invoice: ${invoiceError.message}`);
    }

    if (!invoice) {
      throw new Error(`Invoice ${invoiceId} not found`);
    }

    const balanceDue = invoice.total_amount - totalPaid;

    // 3. Determine new status
    let newStatus;
    if (balanceDue <= 0) {
      newStatus = "PAID";
    } else if (totalPaid > 0) {
      newStatus = "PARTIALLY PAID";
    } else {
      // Check if overdue
      const dueDate = new Date(invoice.due_date);
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      dueDate.setHours(0, 0, 0, 0);

      if (dueDate < today) {
        newStatus = "OVERDUE";
      } else {
        // Use existing status if it's Draft/Invoiced/Sent, otherwise default to Invoiced
        const currentStatus = invoice.status;
        if (["DRAFT", "INVOICED", "SENT"].includes(currentStatus)) {
          newStatus = currentStatus;
        } else {
          newStatus = "INVOICED";
        }
      }
    }

    // 4. Update invoice
    const { error: updateError } = await supabase
      .from("invoices")
      .update({
        balance_due: balanceDue,
        status: newStatus,
        updated_at: new Date().toISOString(),
      })
      .eq("id", invoiceId);

    if (updateError) {
      throw new Error(`Failed to update invoice: ${updateError.message}`);
    }

    console.log(
      `[Invoice Balance] Invoice ${invoiceId} recalculated: balance_due=${balanceDue}, status=${newStatus}`,
    );
  } catch (error) {
    console.error(
      `Error recalculating invoice balance for invoice ${invoiceId}:`,
      error,
    );
    throw error;
  }
}

export { recalculateInvoiceBalance };
