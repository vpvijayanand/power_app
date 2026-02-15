// Main JavaScript for Algo Trading Platform

// Auto-hide flash messages after 5 seconds
document.addEventListener('DOMContentLoaded', function() {
    const alerts = document.querySelectorAll('.alert');
    
    alerts.forEach(alert => {
        setTimeout(() => {
            alert.style.opacity = '0';
            alert.style.transition = 'opacity 0.5s ease';
            
            setTimeout(() => {
                alert.remove();
            }, 500);
        }, 5000);
    });
});

// Form validation helper
function validateForm(formId) {
    const form = document.getElementById(formId);
    if (!form) return true;
    
    const inputs = form.querySelectorAll('input[required], select[required]');
    let isValid = true;
    
    inputs.forEach(input => {
        if (!input.value.trim()) {
            isValid = false;
            input.style.borderColor = 'var(--accent-red)';
        } else {
            input.style.borderColor = 'var(--border-color)';
        }
    });
    
    return isValid;
}

// Confirm before deactivating user
document.addEventListener('DOMContentLoaded', function() {
    const deactivateForms = document.querySelectorAll('form[action*="toggle-user"]');
    
    deactivateForms.forEach(form => {
        form.addEventListener('submit', function(e) {
            const button = form.querySelector('button');
            const action = button.textContent.trim();
            
            if (action === 'Deactivate') {
                if (!confirm('Are you sure you want to deactivate this user?')) {
                    e.preventDefault();
                }
            }
        });
    });
});
