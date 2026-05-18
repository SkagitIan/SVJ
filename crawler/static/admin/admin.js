document.querySelectorAll('[data-stop-propagation]').forEach((element) => {
  element.addEventListener('click', (event) => {
    event.stopPropagation();
  });
});

document.querySelectorAll('form[data-confirm]').forEach((form) => {
  form.addEventListener('submit', (event) => {
    if (!window.confirm(form.dataset.confirm)) {
      event.preventDefault();
      return;
    }
    if (form.dataset.confirmNext && !window.confirm(form.dataset.confirmNext)) {
      event.preventDefault();
    }
  });
});

document.querySelectorAll('[data-open-dialog]').forEach((button) => {
      button.addEventListener('click', () => {
        const dialog = document.getElementById(button.dataset.openDialog);
        if (dialog && typeof dialog.showModal === 'function') dialog.showModal();
      });
    });

    document.querySelectorAll('[data-close-dialog]').forEach((button) => {
      button.addEventListener('click', () => {
        const dialog = button.closest('dialog');
        if (dialog) dialog.close();
      });
    });

    document.querySelectorAll('dialog').forEach((dialog) => {
      dialog.addEventListener('click', (event) => {
        if (event.target === dialog) dialog.close();
      });
    });

    const jobDialog = document.getElementById('job-detail-dialog');
    const jobTitle = document.getElementById('job-modal-title');
    const jobMeta = document.getElementById('job-modal-meta');
    const jobDescription = document.getElementById('job-modal-description');
    const jobApply = document.getElementById('job-modal-apply');
    let previewCloseTimer = null;
    const closeJobPreviews = () => {
      document.querySelectorAll('.job-preview.is-open').forEach((preview) => preview.classList.remove('is-open'));
    };
    const positionJobPreview = (button, preview) => {
      const buttonRect = button.getBoundingClientRect();
      preview.classList.add('is-open');
      const previewRect = preview.getBoundingClientRect();
      const gap = 8;
      let left = buttonRect.left;
      let top = buttonRect.bottom + gap;
      if (left + previewRect.width > window.innerWidth - 16) {
        left = window.innerWidth - previewRect.width - 16;
      }
      if (top + previewRect.height > window.innerHeight - 16) {
        top = Math.max(16, buttonRect.top - previewRect.height - gap);
      }
      preview.style.left = `${Math.max(16, left)}px`;
      preview.style.top = `${Math.max(16, top)}px`;
    };
    document.querySelectorAll('.job-title-link').forEach((button) => {
      const preview = button.parentElement.querySelector('.job-preview');
      if (preview) {
        button.addEventListener('mouseenter', () => {
          if (window.matchMedia('(max-width: 1080px)').matches) return;
          clearTimeout(previewCloseTimer);
          closeJobPreviews();
          positionJobPreview(button, preview);
        });
        button.addEventListener('mouseleave', () => {
          previewCloseTimer = setTimeout(closeJobPreviews, 180);
        });
        preview.addEventListener('mouseenter', () => clearTimeout(previewCloseTimer));
        preview.addEventListener('mouseleave', () => {
          previewCloseTimer = setTimeout(closeJobPreviews, 120);
        });
      }
      button.addEventListener('click', () => {
        if (!jobDialog) return;
        closeJobPreviews();
        jobTitle.textContent = button.dataset.jobTitle || 'Job Details';
        jobMeta.textContent = [
          button.dataset.jobCompany,
          button.dataset.jobLocation,
          button.dataset.jobMeta,
        ].filter(Boolean).join(' | ');
        jobDescription.textContent = button.dataset.jobDescription || 'No description captured yet.';
        if (button.dataset.jobUrl) {
          jobApply.href = button.dataset.jobUrl;
          jobApply.hidden = false;
        } else {
          jobApply.hidden = true;
        }
        jobDialog.showModal();
      });
    });
    window.addEventListener('scroll', closeJobPreviews, true);
    window.addEventListener('resize', closeJobPreviews);
