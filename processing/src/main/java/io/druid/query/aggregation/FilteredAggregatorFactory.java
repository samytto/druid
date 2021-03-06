/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class FilteredAggregatorFactory implements AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x9;

  private final String name;
  private final AggregatorFactory delegate;
  private final DimFilter filter;

  public FilteredAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("filter") DimFilter filter
  )
  {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(filter);
    Preconditions.checkArgument(
        filter instanceof SelectorDimFilter ||
        (filter instanceof NotDimFilter && ((NotDimFilter) filter).getField() instanceof SelectorDimFilter),
        "FilteredAggregator currently only supports filters of type 'selector' and their negation"
    );

    this.name = name;
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final Aggregator aggregator = delegate.factorize(metricFactory);
    final Pair<DimensionSelector, IntPredicate> selectorPredicatePair = makeFilterPredicate(
        filter,
        metricFactory
    );
    return new FilteredAggregator(name, selectorPredicatePair.lhs, selectorPredicatePair.rhs, aggregator);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BufferAggregator aggregator = delegate.factorizeBuffered(metricFactory);
    final Pair<DimensionSelector, IntPredicate> selectorPredicatePair = makeFilterPredicate(
        filter,
        metricFactory
    );
    return new FilteredBufferAggregator(selectorPredicatePair.lhs, selectorPredicatePair.rhs, aggregator);
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegate.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return delegate.finalizeComputation(object);
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] filterCacheKey = filter.getCacheKey();
    byte[] aggregatorCacheKey = delegate.getCacheKey();
    return ByteBuffer.allocate(1 + filterCacheKey.length + aggregatorCacheKey.length)
                     .put(CACHE_TYPE_ID)
                     .put(filterCacheKey)
                     .put(aggregatorCacheKey)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return delegate.getAggregatorStartValue();
  }

  @JsonProperty
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
  }

  private static Pair<DimensionSelector, IntPredicate> makeFilterPredicate(
      final DimFilter dimFilter,
      final ColumnSelectorFactory metricFactory
  )
  {
    final SelectorDimFilter selector;
    if (dimFilter instanceof NotDimFilter) {
      // we only support NotDimFilter with Selector filter
      selector = (SelectorDimFilter) ((NotDimFilter) dimFilter).getField();
    } else if (dimFilter instanceof SelectorDimFilter) {
      selector = (SelectorDimFilter) dimFilter;
    } else {
      throw new ISE("Unsupported DimFilter type [%d]", dimFilter.getClass());
    }

    final DimensionSelector dimSelector = metricFactory.makeDimensionSelector(selector.getDimension());
    final int lookupId = dimSelector.lookupId(selector.getValue());
    final IntPredicate predicate;
    if (dimFilter instanceof NotDimFilter) {
      predicate = new IntPredicate()
      {
        @Override
        public boolean apply(int value)
        {
          return lookupId != value;
        }
      };
    } else {
      predicate = new IntPredicate()
      {
        @Override
        public boolean apply(int value)
        {
          return lookupId == value;
        }
      };
    }
    return Pair.of(dimSelector, predicate);
  }

}
