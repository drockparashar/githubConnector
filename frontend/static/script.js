let currentPage = 1;
const TOTAL_PAGES = 2;

// Function to update the step indicators in the sidebar
function updateSteps() {
    const steps = document.querySelectorAll('.step');
    steps.forEach((step, index) => {
        const stepNumber = index + 1;
        step.classList.remove('active', 'completed');
        if (stepNumber === currentPage) {
            step.classList.add('active');
        } else if (stepNumber < currentPage) {
            step.classList.add('completed');
        }
    });
}

// Function to navigate to a specific page
function goToPage(pageNumber) {
    if (pageNumber < 1 || pageNumber > TOTAL_PAGES) return;

    // Prevent moving forward if connection isn't tested
    if (pageNumber > 1 && !sessionStorage.getItem('authenticationComplete')) {
        return;
    }

    document.querySelectorAll('.page').forEach(page => page.classList.remove('active'));
    document.querySelectorAll('.nav-buttons').forEach(nav => nav.style.display = 'none');

    document.getElementById(`page${pageNumber}`).classList.add('active');
    document.getElementById(`page${pageNumber}-nav`).style.display = 'flex';

    currentPage = pageNumber;
    updateSteps();
}

// Function to go to the next page
async function nextPage() {
    if (currentPage === 1) {
        if (!sessionStorage.getItem('authenticationComplete')) {
            // Automatically run test if not already done
            const success = await testConnection();
            if (!success) return;
        }
    }
    
    if (currentPage < TOTAL_PAGES) {
        goToPage(currentPage + 1);
    }
}

// Function to go to the previous page
function previousPage() {
    if (currentPage > 1) {
        goToPage(currentPage - 1);
    }
}

// Function to test the API connection with the provided token
async function testConnection() {
    const testButton = document.querySelector('.test-connection');
    const errorElement = document.getElementById('connectionError');
    const nextButton = document.getElementById('nextButton');

    testButton.disabled = true;
    testButton.textContent = 'Testing...';
    errorElement.classList.remove('visible');

    try {
        const payload = {
            credentials: {
                token: document.getElementById('token').value
            }
        };

        const response = await fetch('/workflows/v1/auth', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.message || 'Authentication failed. Check your token.');
        }
        
        // Success
        testButton.innerHTML = `Connection Successful <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" width="20" height="20" style="margin-left: 8px"><path fill-rule="evenodd" d="M8.603 3.799A4.49 4.49 0 0112 2.25c1.357 0 2.573.6 3.397 1.549a4.49 4.49 0 013.498 1.307 4.491 4.491 0 011.307 3.497A4.49 4.49 0 0121.75 12a4.49 4.49 0 01-1.549 3.397 4.491 4.491 0 01-1.307 3.497 4.491 4.491 0 01-3.497 1.307A4.49 4.49 0 0112 21.75a4.49 4.49 0 01-3.397-1.549 4.49 4.49 0 01-3.498-1.306 4.491 4.491 0 01-1.307-3.498A4.49 4.49 0 012.25 12c0-1.357.6-2.573 1.549-3.397a4.49 4.49 0 011.307-3.497 4.49 4.49 0 013.497-1.307zm7.007 6.387a.75.75 0 10-1.22-.872l-3.236 4.53L9.53 12.22a.75.75 0 00-1.06 1.06l2.25 2.25a.75.75 0 001.14-.094l3.75-5.25z" clip-rule="evenodd" /></svg>`;
        testButton.classList.add('success');
        nextButton.disabled = false;
        sessionStorage.setItem('authenticationComplete', 'true');
        return true;

    } catch (error) {
        errorElement.textContent = error.message;
        errorElement.classList.add('visible');
        testButton.classList.remove('success');
        nextButton.disabled = true;
        sessionStorage.removeItem('authenticationComplete');
        return false;
    } finally {
        testButton.disabled = false;
        if (!testButton.classList.contains('success')) {
            testButton.textContent = 'Test Connection';
        }
    }
}

// Function to run pre-flight checks
async function runPreflightChecks() {
    const checkButton = document.getElementById("runPreflightChecks");
    const resultsContainer = document.querySelector(".preflight-content");
    
    checkButton.disabled = true;
    checkButton.textContent = "Checking...";
    resultsContainer.innerHTML = "";

    try {
        const payload = {
            credentials: {
                token: document.getElementById("token").value,
            },
            metadata: {
                owner: document.getElementById("owner").value,
            },
        };

        const response = await fetch(`/workflows/v1/check`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.message || 'Preflight check failed.');
        }
        
        const resultDiv = document.createElement("div");
        resultDiv.className = "check-result";
        resultDiv.innerHTML = `
            <div class="check-status success">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"><path fill-rule="evenodd" d="M2.25 12c0-5.385 4.365-9.75 9.75-9.75s9.75 4.365 9.75 9.75-4.365 9.75-9.75 9.75S2.25 17.385 2.25 12zm13.36-1.814a.75.75 0 10-1.22-.872l-3.236 4.53L9.53 12.22a.75.75 0 00-1.06 1.06l2.25 2.25a.75.75 0 001.14-.094l3.75-5.25z" clip-rule="evenodd" /></svg>
                <span>${result.message || 'Checks passed successfully.'}</span>
            </div>`;
        resultsContainer.appendChild(resultDiv);

    } catch (error) {
        const errorDiv = document.createElement("div");
        errorDiv.className = "check-result";
        errorDiv.innerHTML = `
            <div class="check-status error">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"><path fill-rule="evenodd" d="M12 2.25c-5.385 0-9.75 4.365-9.75 9.75s4.365 9.75 9.75 9.75 9.75-4.365 9.75-9.75S17.385 2.25 12 2.25zm-1.72 6.97a.75.75 0 10-1.06 1.06L10.94 12l-1.72 1.72a.75.75 0 101.06 1.06L12 13.06l1.72 1.72a.75.75 0 101.06-1.06L13.06 12l1.72-1.72a.75.75 0 10-1.06-1.06L12 10.94l-1.72-1.72z" clip-rule="evenodd" /></svg>
                <span>${error.message}</span>
            </div>`;
        resultsContainer.appendChild(errorDiv);
    } finally {
        checkButton.disabled = false;
        checkButton.textContent = "Check";
    }
}

// Function to handle starting the main workflow
async function handleRunWorkflow() {
    const runButton = document.querySelector("#runWorkflowButton");
    const modal = document.getElementById("successModal");

    runButton.disabled = true;
    runButton.textContent = "Starting...";

    try {
        const connectionName = document.getElementById("connectionName").value;
        if (!connectionName.trim()) {
            throw new Error("Connection Name is required.");
        }

        const tenantId = window.env.TENANT_ID || "default";
        const appName = window.env.APP_NAME || "sourcesense";
        const currentEpoch = Math.floor(Date.now() / 1000);

        const payload = {
            credentials: {
                token: document.getElementById("token").value,
            },
            connection: {
                connection_name: connectionName,
                connection_qualified_name: `${tenantId}/${appName}/${currentEpoch}`,
            },
            metadata: {
                owner: document.getElementById("owner").value,
            },
        };

        const response = await fetch(`/workflows/v1/start`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.message || "Failed to start workflow");
        }
        
        runButton.textContent = "Started Successfully";
        runButton.classList.add("success");
        modal.classList.add("show");

    } catch (error) {
        console.error("Failed to start workflow:", error);
        runButton.textContent = "Failed to Start";
        runButton.classList.add("error");
        // You can add an error message display here if needed
    } finally {
        setTimeout(() => {
            runButton.disabled = false;
            runButton.textContent = "Run";
            runButton.classList.remove("success", "error");
            modal.classList.remove("show");
        }, 5000);
    }
}


// Setup all event listeners when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Clear session storage on page load to ensure a fresh start
    sessionStorage.removeItem('authenticationComplete');

    // Attach event listener for the preflight check button
    document.getElementById('runPreflightChecks').addEventListener('click', runPreflightChecks);

    // Attach event listener for the main run workflow button
    document.getElementById('runWorkflowButton').addEventListener('click', handleRunWorkflow);
    
    // Add input listeners to reset connection test status
    ['token', 'owner'].forEach(id => {
        document.getElementById(id).addEventListener('input', () => {
            sessionStorage.removeItem('authenticationComplete');
            const testButton = document.querySelector('.test-connection');
            testButton.classList.remove('success');
            testButton.textContent = 'Test Connection';
            document.getElementById('nextButton').disabled = true;
            document.getElementById('connectionError').classList.remove('visible');
        });
    });
});
