const serverUrl = "http://localhost:5000/"
const searchInput = document.getElementById('search-bar-input');
const searchResults = document.getElementById('search-results');

searchInput.addEventListener('keyup', (event) => {
    if (event.key === 'Enter') {
        const searchTerm = searchInput.value;
        getSearchResults(searchTerm);
    }
});

function getSearchResults(movieId) {
    fetch(`${serverUrl}api/recommendation/${movieId}`)
        .then(response => response.json())
        .then(movies => {
            searchResults.innerHTML = '';
            for (const movie of movies) {
                const itemElement = document.createElement('div');
                itemElement.classList.add("ws-tile-three-line-textLeft");

                const itemTitle = document.createElement('p');
                itemTitle.classList.add("list-p")
                itemTitle.textContent = movie["title"];
                itemElement.appendChild(itemTitle);

                const itemReleaseDate = document.createElement('p');
                itemReleaseDate.classList.add("list-p")
                itemReleaseDate.textContent = movie["release_date"];
                itemElement.appendChild(itemReleaseDate);

                searchResults.appendChild(itemElement);
            }
        });
}
