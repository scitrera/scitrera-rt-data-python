import pytest
import logging
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError, IntegrityError
from scitrera_rt_data.sql.sqlalchemy_ import SQLAlchemyEntityClass

def test_sqlalchemy_entity_retry_success():
    mock_engine = MagicMock()
    entity = SQLAlchemyEntityClass(engine=mock_engine, max_retries=3, initial_delay=0.01)
    
    activity_fn = MagicMock(return_value="success")
    result = entity.retry_wrapper(activity_fn)
    
    assert result == "success"
    assert activity_fn.call_count == 1

def test_sqlalchemy_entity_retry_on_operational_error():
    mock_engine = MagicMock()
    entity = SQLAlchemyEntityClass(engine=mock_engine, max_retries=2, initial_delay=0.01)
    
    activity_fn = MagicMock(side_effect=[OperationalError(None, None, "transient"), "success"])
    
    with patch("time.sleep", return_value=None):
        result = entity.retry_wrapper(activity_fn)
    
    assert result == "success"
    assert activity_fn.call_count == 2

def test_sqlalchemy_entity_retry_exhausted():
    mock_engine = MagicMock()
    entity = SQLAlchemyEntityClass(engine=mock_engine, max_retries=2, initial_delay=0.01)
    
    activity_fn = MagicMock(side_effect=OperationalError(None, None, "transient"))
    
    with patch("time.sleep", return_value=None):
        with pytest.raises(ConnectionError, match="Failed to execute query after 3 attempts"):
            entity.retry_wrapper(activity_fn)
    
    assert activity_fn.call_count == 3

def test_sqlalchemy_entity_integrity_error_no_retry():
    mock_engine = MagicMock()
    entity = SQLAlchemyEntityClass(engine=mock_engine, max_retries=3, initial_delay=0.01)
    
    activity_fn = MagicMock(side_effect=IntegrityError(None, None, "integrity"))
    
    with pytest.raises(IntegrityError):
        entity.retry_wrapper(activity_fn)
    
    assert activity_fn.call_count == 1

def test_sqlalchemy_entity_unexpected_error_no_retry():
    mock_engine = MagicMock()
    entity = SQLAlchemyEntityClass(engine=mock_engine, max_retries=3, initial_delay=0.01)
    
    activity_fn = MagicMock(side_effect=ValueError("logical error"))
    
    with pytest.raises(ValueError, match="logical error"):
        entity.retry_wrapper(activity_fn)
    
    assert activity_fn.call_count == 1
