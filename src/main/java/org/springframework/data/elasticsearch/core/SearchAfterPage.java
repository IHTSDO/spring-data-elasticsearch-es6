
package org.springframework.data.elasticsearch.core;

import org.springframework.data.domain.Page;

public interface SearchAfterPage<T> extends Page<T> {

    Object[] getSearchAfter();

}
