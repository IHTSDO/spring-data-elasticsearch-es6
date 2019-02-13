package org.springframework.data.elasticsearch.core;

import org.springframework.data.domain.AbstractPageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.lang.Nullable;

public class SearchAfterPageRequest extends AbstractPageRequest {
	
	private static final long serialVersionUID = -6007427904873239857L;
	private final Sort sort;
	private final String searchAfter;
	
	SearchAfterPageRequest (String searchAfter, int limit, Sort sort) {
		super (0, limit);
		this.searchAfter = searchAfter;
		this.sort = sort;
	}

	/**
	 * Creates a new unsorted {@link SearchAfterPageRequest}.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @since 2.0
	 */
	public static SearchAfterPageRequest of(String searchAfter, int size) {
		return of(searchAfter, size, Sort.unsorted());
	}

	/**
	 * Creates a new {@link SearchAfterPageRequest} with sort parameters applied.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @param sort must not be {@literal null}.
	 * @since 2.0
	 */
	public static SearchAfterPageRequest of(String searchAfter, int size, Sort sort) {
		return new SearchAfterPageRequest(searchAfter, size, sort);
	}

	/**
	 * Creates a new {@link SearchAfterPageRequest} with sort direction and properties applied.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @param direction must not be {@literal null}.
	 * @param properties must not be {@literal null}.
	 * @since 2.0
	 */
	public static SearchAfterPageRequest of(String searchAfter, int size, Direction direction, String... properties) {
		return of(searchAfter, size, Sort.by(direction, properties));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Pageable#getSort()
	 */
	public Sort getSort() {
		return sort;
	}
	
	public String getSearchAfter() {
		return searchAfter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Pageable#next()
	 */
	public Pageable next() {
		//TODO Work out how to calculate the next pageable from here
		throw new RuntimeException ("HERE!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.AbstractPageRequest#previous()
	 */
	public SearchAfterPageRequest previous() {
		//TODO Work out how to calculate the next pageable from here
		throw new RuntimeException ("HERE!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.domain.Pageable#first()
	 */
	public Pageable first() {
		//TODO Work out how to calculate the next pageable from here
		throw new RuntimeException ("HERE!");
	}
	
	@Override
	public int getPageNumber() {
		throw new RuntimeException ("We're not working with page numbers here.");
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof SearchAfterPageRequest)) {
			return false;
		}

		SearchAfterPageRequest that = (SearchAfterPageRequest) obj;

		return super.equals(that) && this.sort.equals(that.sort);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 31 * searchAfter.hashCode() + sort.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Search After Page request [searchAfter: %s, size %d, sort: %s]", searchAfter, getPageSize(), sort);
	}
}
