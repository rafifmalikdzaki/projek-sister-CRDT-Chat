from crdt import *

def test_crdt():
    logger.info("Starting CRDT test...")

    # Initialize two CRDT documents to simulate two clients
    doc1 = CRDTDocument(site_id="client1")
    doc2 = CRDTDocument(site_id="client2")

    # Simulate operations on doc1
    op1 = doc1.insert_char_at_offset(0, 'H')
    op2 = doc1.insert_char_at_offset(1, 'i')

    # Simulate operations on doc2
    op3 = doc2.insert_char_at_offset(0, '!')
    op4 = doc2.insert_char_at_offset(1, ' ')

    # Apply operations from doc1 to doc2
    doc2.apply_remote_operation(op1)
    doc2.apply_remote_operation(op2)

    # Apply operations from doc2 to doc1
    doc1.apply_remote_operation(op3)
    doc1.apply_remote_operation(op4)

    # Log final states
    logger.info(f"Final text in doc1: {doc1.get_text()}")
    logger.info(f"Final text in doc2: {doc2.get_text()}")

    # Assert consistency
    assert doc1.get_text() == doc2.get_text(), "Documents are not consistent!"
    logger.info("CRDT test passed!")

def complex_crdt_test():
    logger.info("Starting complex CRDT test...")

    # Initialize three CRDT documents to simulate three clients
    doc1 = CRDTDocument(site_id="client1")
    doc2 = CRDTDocument(site_id="client2")
    doc3 = CRDTDocument(site_id="client3")

    # Scenario 1: Concurrent Insertions
    logger.info("Scenario 1: Concurrent Insertions")
    op1 = doc1.insert_char_at_offset(0, 'H')
    op2 = doc2.insert_char_at_offset(0, 'W')
    op3 = doc3.insert_char_at_offset(0, '!')

    # Apply all operations across all documents
    for doc in [doc2, doc3]:
        doc.apply_remote_operation(op1)
    for doc in [doc1, doc3]:
        doc.apply_remote_operation(op2)
    for doc in [doc1, doc2]:
        doc.apply_remote_operation(op3)

    logger.info(f"After Scenario 1 - doc1: {doc1.get_text()}, doc2: {doc2.get_text()}, doc3: {doc3.get_text()}")

    # Scenario 2: Deletions
    logger.info("Scenario 2: Deletions")
    op4 = doc1.delete_char_at_offset(1)  # Delete 'W'
    op5 = doc2.delete_char_at_offset(2)  # Delete '!'

    # Apply deletions across all documents
    for doc in [doc2, doc3]:
        doc.apply_remote_operation(op4)
    for doc in [doc1, doc3]:
        doc.apply_remote_operation(op5)

    logger.info(f"After Scenario 2 - doc1: {doc1.get_text()}, doc2: {doc2.get_text()}, doc3: {doc3.get_text()}")

    # Scenario 3: Mixed Operations
    logger.info("Scenario 3: Mixed Operations")
    op6 = doc1.insert_char_at_offset(1, 'e')  # Insert 'e' after 'H'
    op7 = doc2.insert_char_at_offset(0, 'A')  # Insert 'A' at the beginning
    op8 = doc3.delete_char_at_offset(0)  # Delete 'A' (from doc2)

    # Apply operations across all documents
    for doc in [doc2, doc3]:
        doc.apply_remote_operation(op6)
    for doc in [doc1, doc3]:
        doc.apply_remote_operation(op7)
    for doc in [doc1, doc2]:
        doc.apply_remote_operation(op8)

    logger.info(f"After Scenario 3 - doc1: {doc1.get_text()}, doc2: {doc2.get_text()}, doc3: {doc3.get_text()}")

    # Scenario 4: Complex Interleaving
    logger.info("Scenario 4: Complex Interleaving")
    op9 = doc1.insert_char_at_offset(2, 'l')  # Insert 'l'
    op10 = doc2.insert_char_at_offset(3, 'o')  # Insert 'o'
    op11 = doc3.insert_char_at_offset(1, 'A')  # Reinsert 'A'

    # Apply operations across all documents
    for doc in [doc2, doc3]:
        doc.apply_remote_operation(op9)
    for doc in [doc1, doc3]:
        doc.apply_remote_operation(op10)
    for doc in [doc1, doc2]:
        doc.apply_remote_operation(op11)

    logger.info(f"After Scenario 4 - doc1: {doc1.get_text()}, doc2: {doc2.get_text()}, doc3: {doc3.get_text()}")

    # Final State Verification
    final_text1 = doc1.get_text()
    final_text2 = doc2.get_text()
    final_text3 = doc3.get_text()
    logger.info(f"Final text in doc1: {final_text1}")
    logger.info(f"Final text in doc2: {final_text2}")
    logger.info(f"Final text in doc3: {final_text3}")

    # Assert consistency
    assert final_text1 == final_text2 == final_text3, "Documents are not consistent!"
    logger.info("Complex CRDT test passed!")


if __name__ == "__main__":
    complex_crdt_test()
    # test_crdt()
